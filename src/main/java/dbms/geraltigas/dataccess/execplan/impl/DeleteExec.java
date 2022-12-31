package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.expression.Expression;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.indexs.IndexPageHeader;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.transaction.changelog.impl.IndexChangeLog;
import dbms.geraltigas.transaction.changelog.impl.RecordChangeLog;
import dbms.geraltigas.utils.DataDump;
import dbms.geraltigas.utils.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class DeleteExec implements ExecPlan {

    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;

    @Autowired
    LockManager lockManager;

    String tableName;
    Expression whereExpression;
    private long threadId;
    boolean isTxn;
    Executor transactionExecutor;

    IndexUseType indexUseType = IndexUseType.NONE;

    enum IndexUseType {
        NONE,
        OR_ALL,
        AND_ONE
    }

    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        List<String> res = new ArrayList<>();

        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);
        long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,0);
        lockManager.lockWrite(tableHeaderId, threadId);
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int tableLength = tableHeader.getTableLength();
        int deleteNum = 0;


        Pair<List<String>,List<String>> pair = tableBuffer.getIndexNameAndIndexColumnNameList(tableName);
        List<String> indexNameList = pair.getFirst();
        List<String> indexColumnNameList = pair.getSecond();
        Set<String> indexColumNameSet = new TreeSet<>(indexColumnNameList);
        List<Integer> pageIdList = new ArrayList<>();
        // we only need to support const value operation in delete
        Pair<List<String>,List<Pair<Expression.Op,Object>>> columnNameAndValueList = getColumnNameAndValueList(whereExpression);
        List<String> whereColumnNameList = columnNameAndValueList.getFirst();
        List<Pair<Expression.Op,Object>> valueList = columnNameAndValueList.getSecond();
        Set<String> whereColumnNameSet = new TreeSet<>(whereColumnNameList);

        // set and operation
        Set<String> haveIndexColumnName = new TreeSet<>(indexColumNameSet);
        haveIndexColumnName.retainAll(whereColumnNameSet);
        isSimpleWhereExpression(whereExpression,whereColumnNameSet,haveIndexColumnName);

        switch (indexUseType){
            case NONE -> {
                for (int index = 0; index < tableLength; index++) {
                    long pageLockId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,index+1);
                    lockManager.lockWrite(pageLockId, threadId);
                    PageHeader pageHeader = diskManager.getPageHeader(tableName, index+1);
                    int recordNum = pageHeader.getRecordNum();
                    for (int i = 0; i < recordNum;i++) {
                        byte[] record = diskManager.getOneRecord(tableName, index+1, i);
                        if (record[0] == 1) {
                            // judge the record is valid
                            if (whereExpression != null) {
                                if (whereExpression.evalNoAlias(record, tableDefine)) {
                                    if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, index+1, i, record.clone()));
                                    record[0] = 0;
                                    deleteNum++;
                                    diskManager.setOneRecord(tableName,index+1,i,record);
                                    deleteIndexData(indexColumnNameList,record,tableDefine,index+1,i);
                                }
                            }else {
                                if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, index+1, i, record.clone()));
                                record[0] = 0;
                                deleteNum++;
                                diskManager.setOneRecord(tableName,index+1,i,record);
                                deleteIndexData(indexColumnNameList,record,tableDefine,index+1,i);
                            }
                        }
                    }
                }
                break;
            }
            case OR_ALL -> {
                List<Set<Integer>> pageIndexSetList = new ArrayList<>();
                for (String columnName : haveIndexColumnName) {
                    Set<Integer> pageIndexSet = new TreeSet<>();
                    int temp = 0;
                    for (String whereColumbName : whereColumnNameList) {
                        if (whereColumbName.equals(columnName)) {
                            break;
                        }
                        temp++;
                    }
                    Object value = valueList.get(temp).getSecond();
                    int hash = value.toString().hashCode();
                    int index = indexColumnNameList.indexOf(columnName);
                    String indexName = indexNameList.get(index);
                    long indexHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
                    lockManager.lockRead(indexHeaderId, threadId);
                    IndexHeader indexHeader = diskManager.getIndexHeader(tableName, indexName);
                    int hashIndex = hash % indexHeader.getIndexHashArraySize();
                    long indexDataLockId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,hashIndex+1);
                    lockManager.lockRead(indexDataLockId, threadId);
                    IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hashIndex+1);
                    int indexDataNum = indexPageHeader.getIndexNum();
                    List<TableDefine.Type> typeList = new ArrayList<>();
                    List<TableDefine.Type> tableTypeList = tableDefine.getColTypes();
                    List<String> colNameList = tableDefine.getColNames();
                    int colIndex = colNameList.indexOf(columnName);
                    typeList.add(tableTypeList.get(colIndex));
                    typeList.add(TableDefine.Type.INTEGER); // page index
                    typeList.add(TableDefine.Type.INTEGER); // record index
                    List<List<String>> attrValues = new ArrayList<>();

                    for (int i = 0; i < indexDataNum; i++) {
                        byte[] indexData = diskManager.getOneIndexData(tableName, hashIndex+1,indexName, i,InsertExec.CalculateLength(typeList,attrValues));
                        if (indexData[0] == 1) {
                            List<Object> valueList1 = DataDump.load(typeList, indexData,0);
                            Integer pageIndex = (Integer) valueList1.get(1);
                            pageIndexSet.add(pageIndex);
                        }
                    }
                    pageIndexSetList.add(pageIndexSet);
                }
                Set<Integer> pageIndexSet = new TreeSet<>();
                for (Set<Integer> set : pageIndexSetList) {
                    pageIndexSet.addAll(set);
                }
                for (Integer pageId : pageIndexSet) {
                    long pageIdLockId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageId);
                    lockManager.lockWrite(pageIdLockId, threadId);
                    PageHeader pageHeader = diskManager.getPageHeader(tableName, pageId);
                    int recordNum1 = pageHeader.getRecordNum();
                    for (int i = 0; i < recordNum1; i++) {
                        byte[] record = diskManager.getOneRecord(tableName, pageId, i);
                        if (record[0] == 1) {
                            if (whereExpression.evalNoAlias(record, tableDefine)) {
                                if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, pageId, i, record.clone()));

                                record[0] = 0;
                                deleteNum++;
                                diskManager.setOneRecord(tableName,pageId,i,record);
                                deleteIndexData(indexColumnNameList,record,tableDefine,pageId,i);
                            }
                        }
                    }
                }
                break;
            }
            case AND_ONE -> {
                List<Set<Integer>> pageIndexSetList = new ArrayList<>();
                for (String columnName : haveIndexColumnName) {
                    Set<Integer> pageIndexSet = new TreeSet<>();
                    int temp = 0;
                    for (String whereColumbName : whereColumnNameList) {
                        if (whereColumbName.equals(columnName)) {
                            break;
                        }
                        temp++;
                    }
                    Object value = valueList.get(temp).getSecond();
                    int hash = value.toString().hashCode();
                    int index = indexColumnNameList.indexOf(columnName);
                    String indexName = indexNameList.get(index);
                    long indexHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
                    lockManager.lockRead(indexHeaderId, threadId);
                    IndexHeader indexHeader = diskManager.getIndexHeader(tableName, indexName);
                    int hashIndex = hash % indexHeader.getIndexHashArraySize();
                    long indexDataLockId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,hashIndex+1);
                    lockManager.lockWrite(indexDataLockId, threadId);
                    IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hashIndex+1);
                    int indexDataNum = indexPageHeader.getIndexNum();
                    List<TableDefine.Type> typeList = new ArrayList<>();
                    List<TableDefine.Type> tableTypeList = tableDefine.getColTypes();
                    List<String> colNameList = tableDefine.getColNames();
                    int colIndex = colNameList.indexOf(columnName);
                    typeList.add(tableTypeList.get(colIndex));
                    typeList.add(TableDefine.Type.INTEGER); // page index
                    typeList.add(TableDefine.Type.INTEGER); // record index
                    List<List<String>> attrValues = new ArrayList<>();

                    for (int i = 0; i < indexDataNum; i++) {
                        byte[] indexData = diskManager.getOneIndexData(tableName, hashIndex+1,indexName, i,InsertExec.CalculateLength(typeList,attrValues));
                        if (indexData[0] == 1) {
                            List<Object> valueList1 = DataDump.load(typeList, indexData,0);
                            Integer pageIndex = (Integer) valueList1.get(1);
                            pageIndexSet.add(pageIndex);
                        }
                    }
                    pageIndexSetList.add(pageIndexSet);
                }
                Set<Integer> pageIndexSet = new TreeSet<>(pageIndexSetList.get(0));
                for (Set<Integer> set : pageIndexSetList) {
                    pageIndexSet.retainAll(set);
                }
                for (Integer pageId : pageIndexSet) {
                    long pageIdLockId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageId);
                    lockManager.lockWrite(pageIdLockId, threadId);
                    PageHeader pageHeader = diskManager.getPageHeader(tableName, pageId);
                    int recordNum1 = pageHeader.getRecordNum();
                    for (int i = 0; i < recordNum1; i++) {
                        byte[] record = diskManager.getOneRecord(tableName, pageId, i);
                        if (record[0] == 1) {
                            if (whereExpression.evalNoAlias(record, tableDefine)) {
                                if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, pageId, i, record.clone()));
                                record[0] = 0;
                                deleteNum++;
                                diskManager.setOneRecord(tableName,pageId,i,record);
                                deleteIndexData(indexColumnNameList,record,tableDefine,pageId,i);
                            }
                        }
                    }
                }
                break;
            }
        }

        res.add("Delete " + deleteNum + " records from table " + tableName);

        if(!isTxn) {
            lockManager.unlockAll(threadId);
        }

        return String.join("\n", res);
    }

    private void deleteIndexData(List<String> indexColumnNames,byte[] recordWithValid,TableDefine tableDefine,int pageIndex,int recordIndex) throws BlockException, DataDirException, IOException, DataTypeException {
        for (String indexColumnName : indexColumnNames) {
            List<Object> valueList = DataDump.load(tableDefine.getColTypes(), recordWithValid,0);
            long indexHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexColumnName,0);
            IndexHeader indexHeader = diskManager.getIndexHeader(tableName, indexColumnName);
            deleteSpecificIndexData(indexColumnName,valueList,tableDefine,indexHeader,pageIndex,recordIndex);
        }
    }

    private void deleteSpecificIndexData(String indexColumnName, List<Object> valueList, TableDefine tableDefine,IndexHeader indexHeader,int pageIndex,int recordIndex) throws BlockException, DataDirException, IOException, DataTypeException {
        int columnNameIndex = tableDefine.getColNames().indexOf(indexColumnName);
        Object value = valueList.get(columnNameIndex);
        int hash = value.toString().hashCode();
        int hashIndex = hash % indexHeader.getIndexHashArraySize();
        long indexDataLockId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexColumnName,hashIndex+1);
        lockManager.lockWrite(indexDataLockId, threadId);
        IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexColumnName, hashIndex+1);
        int indexDataNum = indexPageHeader.getIndexNum();
        List<TableDefine.Type> typeList = new ArrayList<>();
        List<TableDefine.Type> tableTypeList = tableDefine.getColTypes();
        List<String> colNameList = tableDefine.getColNames();
        int colIndex = colNameList.indexOf(indexColumnName);
        typeList.add(tableTypeList.get(colIndex));
        typeList.add(TableDefine.Type.INTEGER); // page index
        typeList.add(TableDefine.Type.INTEGER); // record index
        List<List<String>> attrValues = new ArrayList<>();

        for (int i = 0; i < indexDataNum; i++) {
            byte[] indexData = diskManager.getOneIndexData(tableName, hashIndex+1,indexColumnName, i,InsertExec.CalculateLength(typeList,attrValues));
            if (indexData[0] == 1) {
                List<Object> valueList1 = DataDump.load(typeList, indexData,0);
                Integer pageIndex1 = (Integer) valueList1.get(1);
                Integer recordIndex1 = (Integer) valueList1.get(2);
                if (pageIndex1 == pageIndex && recordIndex1 == recordIndex) {
                    if (isTxn) transactionExecutor.addChangeLog(new IndexChangeLog(tableName, hashIndex+1,indexColumnName, i, InsertExec.CalculateLength(typeList,attrValues),indexData.clone()));
                    indexData[0] = 0;
                    diskManager.setOneIndexData(tableName, hashIndex+1,indexColumnName, i, InsertExec.CalculateLength(typeList,attrValues),indexData);
                    break;
                }
            }
        }
    }


    private boolean isSimpleWhereExpression(Expression whereExpression,Set<String> allUsedColumnNameSet,Set<String> haveIndexColumnName) {
        if (whereExpression == null) {
            return false;
        }
        boolean isSimple = true;
        Pair<Boolean, Expression.Op> pair = isSameOp(whereExpression);
        if (pair.getFirst()) {
            Expression.Op op = pair.getSecond();
            switch (op) {
                case EQUAL, OR -> {
                    indexUseType = IndexUseType.OR_ALL;
                    for (String columnName : allUsedColumnNameSet) {
                        if (!haveIndexColumnName.contains(columnName)) {
                            isSimple = false;
                            indexUseType = IndexUseType.NONE;
                            break;
                        }
                    }
                }
                case AND -> {
                    indexUseType = IndexUseType.AND_ONE;
                    for (String columnName : allUsedColumnNameSet) {
                        if (haveIndexColumnName.contains(columnName)) {
                            break;
                        }
                    }
                }
            }
        }else {
            isSimple = false;
        }
        return isSimple;
    }

    private Pair<Boolean, Expression.Op> isSameOp(Expression whereExpression) {
        if (whereExpression == null) {
            return new Pair<>(true, null);
        }
        if (whereExpression.getOp() == Expression.Op.EQUAL) {
            return new Pair<>(true, Expression.Op.EQUAL);
        }
        Pair<Boolean,Expression.Op> left = isSameOp(whereExpression.getLeft());
        Pair<Boolean,Expression.Op> right = isSameOp(whereExpression.getRight());
        if (right.getFirst() && left.getFirst()) {
            if (right.getSecond() == left.getSecond()) {
                Expression.Op op = right.getSecond();
                boolean isSame = right.getSecond() == whereExpression.getOp();
                if (right.getSecond() == Expression.Op.EQUAL && whereExpression.getOp() != Expression.Op.EQUAL) {
                    isSame = true;
                    op = whereExpression.getOp();
                }
                return new Pair<>(isSame, op);
            }else {
                if ((right.getSecond() == Expression.Op.AND && left.getSecond() == Expression.Op.OR) || (right.getSecond() == Expression.Op.OR && left.getSecond() == Expression.Op.AND)) {
                    return new Pair<>(false, null);
                }else {
                    Expression.Op op = right.getSecond() != Expression.Op.EQUAL ? right.getSecond() : left.getSecond();
                    return new Pair<>(true, op);
                }
            }
        }else {
            return new Pair<>(false, null);
        }
    }

    private Pair<List<String>, List<Pair<Expression.Op,Object>>> getColumnNameAndValueList(Expression whereExpression) {
        List<String> columnNameList = new ArrayList<>();
        List<Pair<Expression.Op,Object>> valueList = new ArrayList<>();
        List<Expression> equelExpressionList = new ArrayList<>();
        addAllEquelExpression(whereExpression, equelExpressionList);
        for (Expression expression : equelExpressionList) {
            if (expression.getLeft().getOp() == Expression.Op.NULL) {
                columnNameList.add(expression.getLeft().getName());
                valueList.add(new Pair<>(expression.getRight().getOp(), expression.getRight().getValue()));
            }else {
                columnNameList.add(expression.getRight().getName());
                valueList.add(new Pair<>(expression.getLeft().getOp(), expression.getLeft().getValue()));
            }
        }
        return new Pair<>(columnNameList, valueList);
    }

    public void addAllEquelExpression(Expression expression,List<Expression> collect) {
        if (expression.getOp() == Expression.Op.EQUAL) {
            collect.add(expression);
        }else {
            addAllEquelExpression(expression.getLeft(), collect);
            addAllEquelExpression(expression.getRight(), collect);
        }
    }

    public DeleteExec(String tableName, Expression whereExpression) {
        this.tableName = tableName;
        this.whereExpression = whereExpression;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
    @Override
    public long getThreadId() {
        return this.threadId;
    }
}
