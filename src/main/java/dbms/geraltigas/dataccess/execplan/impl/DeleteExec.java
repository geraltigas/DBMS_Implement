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
import dbms.geraltigas.utils.IndexUtils;
import dbms.geraltigas.utils.Pair;
import dbms.geraltigas.utils.Printer;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class DeleteExec implements ExecPlan {
    String tableName;
    Expression whereExpression;
    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;

    @Autowired
    LockManager lockManager;

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
    public String execute(String dataPath) throws IOException, FieldNotFoundException, BlockException, DataDirException, DataTypeException {
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

        for (String columnName : whereColumnNameList) {
            int index = tableDefine.getColNames().indexOf(columnName);
            TableDefine.Type type = tableDefine.getColTypes().get(index);
            switch (type) {
                case INTEGER -> {
                    if (!(valueList.get(index).getSecond() instanceof Integer)) {
                        return "Column " + columnName + " type dont match";
                    }
                }
                case FLOAT -> {
                    if(!(valueList.get(index).getSecond() instanceof Float)) {
                        return "Column " + columnName + " type dont match";
                    }
                }
                case VARCHAR -> {
                    if(!(valueList.get(index).getSecond() instanceof String)) {
                        return "Column " + columnName + " type dont match";
                    }
                }
            }
        }

        // set and operation
        Set<String> haveIndexColumnName = new TreeSet<>(indexColumNameSet);
        haveIndexColumnName.retainAll(whereColumnNameSet);
        isSimpleWhereExpression(whereExpression,whereColumnNameSet,haveIndexColumnName);

        Printer.print("indexUseType: " + indexUseType,threadId);

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
                                try {
                                    if (whereExpression.evalNoAlias(record, tableDefine)) {
                                        if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, index+1, i, record.clone()));
                                        record[0] = 0;
                                        deleteNum++;
                                        diskManager.setOneRecord(tableName,index+1,i,record);
                                        deleteIndexData(indexNameList,indexColumnNameList,record,tableDefine,index+1,i);
                                    }
                                }catch (DataTypeException e) {
                                    return e.getMessage();
                                }

                            }else {
                                if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, index+1, i, record.clone()));
                                record[0] = 0;
                                deleteNum++;
                                diskManager.setOneRecord(tableName,index+1,i,record);
                                deleteIndexData(indexNameList,indexColumnNameList,record,tableDefine,index+1,i);
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
                    if (hashIndex < 0) hashIndex += indexHeader.getIndexHashArraySize();
                    long indexDataLockId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,hashIndex+1);
                    lockManager.lockRead(indexDataLockId, threadId);
                    IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hashIndex+1);
                    int indexDataNum = indexPageHeader.getIndexNum();
                    List<TableDefine.Type> tableTypeList = tableDefine.getColTypes();
                    List<String> colNameList = tableDefine.getColNames();
                    int colIndex = colNameList.indexOf(columnName);
                    IndexUtils indexUtils = new IndexUtils(tableTypeList.get(colIndex),tableDefine.getColAttrs().get(colIndex));

                    for (int i = 0; i < indexDataNum; i++) {
                        byte[] indexData = diskManager.getOneIndexData(tableName, hashIndex+1,indexName, i,indexUtils.getIndexDataLength());

                        if (indexData[0] == 1) {
                            List<Object> valueList1 = DataDump.load(indexUtils.getTypeList(), indexData,0);
                            Integer pageIndex = (Integer) valueList1.get(1);
                            pageIndexSet.add(pageIndex);
                        }
                    }
                    pageIndexSetList.add(pageIndexSet);
                    Printer.print("using index in indexName: " + indexName + " ,indexPageIndex: " + hashIndex+1 + " ,data: " + value + " ,chosenPageNum: " + pageIndexSet.size(),threadId);
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
                            try {
                                if (whereExpression.evalNoAlias(record, tableDefine)) {
                                    if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, pageId, i, record.clone()));
                                    record[0] = 0;
                                    deleteNum++;
                                    diskManager.setOneRecord(tableName,pageId,i,record);
                                    deleteIndexData(indexNameList,indexColumnNameList,record,tableDefine,pageId,i);
                                }
                            }catch (DataTypeException e){
                                return e.getMessage();
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
                    if (hashIndex < 0) hashIndex += indexHeader.getIndexHashArraySize();
                    long indexDataLockId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,hashIndex+1);
                    lockManager.lockWrite(indexDataLockId, threadId);
                    IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hashIndex+1);
                    int indexDataNum = indexPageHeader.getIndexNum();
                    List<TableDefine.Type> tableTypeList = tableDefine.getColTypes();
                    List<String> colNameList = tableDefine.getColNames();
                    int colIndex = colNameList.indexOf(columnName);
                    IndexUtils indexUtils = new IndexUtils(tableTypeList.get(colIndex),tableDefine.getColAttrs().get(colIndex));

                    for (int i = 0; i < indexDataNum; i++) {
                        byte[] indexData = diskManager.getOneIndexData(tableName, hashIndex+1,indexName, i,indexUtils.getIndexDataLength());
                        if (indexData[0] == 1) {
                            List<Object> valueList1 = DataDump.load(indexUtils.getTypeList(), indexData,0);
                            Integer pageIndex = (Integer) valueList1.get(1);
                            pageIndexSet.add(pageIndex);
                        }
                    }
                    pageIndexSetList.add(pageIndexSet);
                    Printer.print("using index in indexName: " + indexName + " ,indexPageIndex: " + hashIndex+1 + " ,data: " + value + " ,chosenPageNum: " + pageIndexSet.size(),threadId);
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
                            if (whereExpression == null) {
                                if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, pageId, i, record.clone()));
                                record[0] = 0;
                                deleteNum++;
                                diskManager.setOneRecord(tableName,pageId,i,record);
                                deleteIndexData(indexNameList,indexColumnNameList,record,tableDefine,pageId,i);
                            }else {
                                try {
                                    if (whereExpression.evalNoAlias(record, tableDefine)) {
                                        if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName, pageId, i, record.clone()));
                                        record[0] = 0;
                                        deleteNum++;
                                        diskManager.setOneRecord(tableName,pageId,i,record);
                                        deleteIndexData(indexNameList,indexColumnNameList,record,tableDefine,pageId,i);
                                    }
                                }catch (DataTypeException e){
                                    return e.getMessage();
                                }
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

    private void deleteIndexData(List<String> indexNames,List<String> indexColumnList,byte[] recordWithValid,TableDefine tableDefine,int pageIndex,int recordIndex) throws BlockException, DataDirException, IOException, DataTypeException {
        for (int i = 0 ; i < indexNames.size();i++) {
            String indexName = indexNames.get(i);
            String indexColumnName = indexColumnList.get(i);
            List<Object> valueList = DataDump.load(tableDefine.getColTypes(), recordWithValid,0);
            long indexHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
            lockManager.lockRead(indexHeaderId, threadId);
            IndexHeader indexHeader = diskManager.getIndexHeader(tableName, indexName);
            deleteSpecificIndexData(indexName,indexColumnName,valueList,tableDefine,indexHeader,pageIndex,recordIndex);
        }
    }

    private void deleteSpecificIndexData(String indexName,String indexColumnName, List<Object> valueList, TableDefine tableDefine,IndexHeader indexHeader,int pageIndex,int recordIndex) throws BlockException, DataDirException, IOException, DataTypeException {
        int columnNameIndex = tableDefine.getColNames().indexOf(indexColumnName);
        Object value = valueList.get(columnNameIndex);
        int hash = value.toString().hashCode();
        hash %= indexHeader.getIndexHashArraySize();
        if (hash < 0) hash += indexHeader.getIndexHashArraySize();
        long indexDataLockId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,hash+1);
        lockManager.lockWrite(indexDataLockId, threadId);
        IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hash+1);
        int indexDataNum = indexPageHeader.getIndexNum();
        List<TableDefine.Type> tableTypeList = tableDefine.getColTypes();
        List<String> colNameList = tableDefine.getColNames();
        int colIndex = colNameList.indexOf(indexColumnName);
        IndexUtils indexUtils = new IndexUtils(tableTypeList.get(colIndex),tableDefine.getColAttrs().get(colIndex));

        for (int i = 0; i < indexDataNum; i++) {
            byte[] indexData = diskManager.getOneIndexData(tableName, hash+1,indexName, i,indexUtils.getIndexDataLength());
            if (indexData[0] == 1) {
                List<Object> valueList1 = DataDump.load(indexUtils.getTypeList(), indexData,0);
                Integer pageIndex1 = (Integer) valueList1.get(1);
                Integer recordIndex1 = (Integer) valueList1.get(2);
                if (pageIndex1 == pageIndex && recordIndex1 == recordIndex) {
                    if (isTxn) transactionExecutor.addChangeLog(new IndexChangeLog(tableName, hash+1,indexName, i, indexUtils.getIndexDataLength(),indexData.clone()));
                    indexData[0] = 0;
                    Printer.print("delete index data on indexName: " + indexName + ", \ndata: " + value,threadId);
                    diskManager.setOneIndexData(tableName, hash+1,indexName, i, indexUtils.getIndexDataLength(),indexData);
                    break;
                }
            }
        }
    }


    private boolean isSimpleWhereExpression(Expression whereExpression,Set<String> allUsedColumnNameSet,Set<String> haveIndexColumnName) {
        if (whereExpression == null) {
            return false;
        }
        if (haveIndexColumnName.size() == 0) {
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
                if (right.getSecond() == Expression.Op.EQUAL) {
                    return new Pair<>(true,whereExpression.getOp());
                }else {
                    if (whereExpression.getOp() == right.getSecond()) {
                        return new Pair<>(true,whereExpression.getOp());
                    }else {
                        return new Pair<>(false,null);
                    }
                }
            }else {
                if ((right.getSecond() == Expression.Op.AND && left.getSecond() == Expression.Op.OR) || (right.getSecond() == Expression.Op.OR && left.getSecond() == Expression.Op.AND)) {
                    return new Pair<>(false, null);
                }else {
                    Expression.Op op = right.getSecond() != Expression.Op.EQUAL ? right.getSecond() : left.getSecond();
                    if (op == whereExpression.getOp()) {
                        return new Pair<>(true, op);
                    }else {
                        return new Pair<>(false, null);
                    }
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
            if (expression.getRight().getOp() == expression.getLeft().getOp()) {
                continue;
            }
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
        if (expression == null) {
            return;
        }
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
