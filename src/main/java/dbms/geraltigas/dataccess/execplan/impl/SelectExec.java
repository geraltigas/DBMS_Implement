package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.expression.Expression;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.indexs.IndexPageHeader;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.transaction.changelog.impl.RecordChangeLog;
import dbms.geraltigas.utils.DataDump;
import dbms.geraltigas.utils.Pair;
import dbms.geraltigas.utils.SetIterator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;

import static dbms.geraltigas.buffer.BlockBuffer.BLOCK_SIZE;

public class SelectExec implements ExecPlan { // TODO:  change to lock and index

    List<Expression> expressions;
    private long threadId;

    public static final int SELECT_PAGE_NUMBER = 5;

    List<String> names;
    List<String> tableNames;
    Expression whereExpression;
    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;

    @Autowired
    LockManager lockManager;

    DeleteExec.IndexUseType indexUseType = DeleteExec.IndexUseType.NONE;

    @Override
    public String execute(String dataPath) throws BlockException, DataDirException, IOException, DataTypeException {
        List<String> res = new ArrayList<>();
        addPrintHead(res,names);
        if (tableNames.size() == 1) {
            String tableName = tableNames.get(0);
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

            TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

            switch (indexUseType){
                case NONE -> {
                    long pageHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,0);
                    lockManager.lockRead(pageHeaderId,threadId);
                    int j = diskManager.getTableHeader(tableName).getTableLength();
                    for (int i = 1; i <= j; i++) {
                        pageIdList.add(i);
                    }
                    for (int pageIndex : pageIdList) {
                        long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageIndex);
                        lockManager.lockRead(pageId,threadId);
                        PageHeader pageHeader = diskManager.readPageHeader(tableName, pageIndex);
                        int recordNumT = pageHeader.getRecordNum();
                        for (int recordIndex = 0 ; recordIndex < recordNumT;recordIndex++) {
                            byte[] record = diskManager.readOneRecord(tableName, pageIndex, recordIndex);
                            if (record[0] == 1) {
                                // judge the record is valid
                                if (whereExpression != null) {
                                    whereExpression.eval(record, tableDefine, names, expressions, res);
                                }else {
                                    Expression.nullEval(record, tableDefine, names, expressions, res);
                                }
                            }
                        }
                    }
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
                            byte[] indexData = diskManager.readOneIndexData(tableName, hashIndex+1,indexName, i,InsertExec.CalculateLength(typeList,attrValues));
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
                    for (Integer pageIndex : pageIndexSet) {
                        long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageIndex);
                        lockManager.lockRead(pageId,threadId);
                        PageHeader pageHeader = diskManager.readPageHeader(tableName, pageIndex);
                        int recordNumT = pageHeader.getRecordNum();
                        for (int recordIndex = 0 ; recordIndex < recordNumT;recordIndex++) {
                            byte[] record = diskManager.readOneRecord(tableName, pageIndex, recordIndex);
                            if (record[0] == 1) {
                                // judge the record is valid
                                if (whereExpression != null) {
                                    whereExpression.eval(record, tableDefine, names, expressions, res);
                                }else {
                                    Expression.nullEval(record, tableDefine, names, expressions, res);
                                }
                            }
                        }
                    }
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
                            byte[] indexData = diskManager.readOneIndexData(tableName, hashIndex+1,indexName, i,InsertExec.CalculateLength(typeList,attrValues));
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
                    for (Integer pageIndex : pageIndexSet) {
                        long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageIndex);
                        lockManager.lockRead(pageId,threadId);
                        PageHeader pageHeader = diskManager.readPageHeader(tableName, pageIndex);
                        int recordNumT = pageHeader.getRecordNum();
                        for (int recordIndex = 0 ; recordIndex < recordNumT;recordIndex++) {
                            byte[] record = diskManager.readOneRecord(tableName, pageIndex, recordIndex);
                            if (record[0] == 1) {
                                // judge the record is valid
                                if (whereExpression != null) {
                                    whereExpression.eval(record, tableDefine, names, expressions, res);
                                }else {
                                    Expression.nullEval(record, tableDefine, names, expressions, res);
                                }
                            }
                        }
                    }
                }
            }
        }else
        {
            List<Set<byte[]>> recordSetList = new ArrayList<>();
            for (String tableName : tableNames) {
                Set<byte[]> recordSet = new TreeSet<>(Comparator.comparing(Arrays::toString));
                long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,0);
                lockManager.lockRead(tableHeaderId, threadId);
                TableHeader tableHeader = diskManager.getTableHeader(tableName);
                int pageNum = tableHeader.getTableLength();
                for (int i = 0;i < pageNum;i++) {
                    long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,i+1);
                    lockManager.lockRead(pageId,threadId);
                    PageHeader pageHeader = diskManager.readPageHeader(tableName, i+1);
                    int recordNumT = pageHeader.getRecordNum();
                    for (int j = 0;j < recordNumT;j++) {
                        byte[] record = diskManager.readOneRecord(tableName, i+1, j);
                        if (record[0] == 1) {
                            recordSet.add(record);
                        }
                    }
                }
                recordSetList.add(recordSet);
            }
            SetIterator<byte[]> from = new SetIterator<>(recordSetList.get(0),null);
            SetIterator<byte[]> to = new SetIterator<>(recordSetList.get(1),from);
            Iterator<Set<byte[]>> iterator = recordSetList.iterator();
            iterator.next();
            while (iterator.hasNext()) {
                from = to;
                to = new SetIterator<>(iterator.next(),to);
            }
            List<List<TableDefine.Type>> types = new ArrayList<>();
            List<TableDefine.Type> typeList = new ArrayList<>();
            List<String> colNameList = new ArrayList<>();
            for (String tableName : tableNames) {
                types.add(tableBuffer.getTableDefine(tableName).getColTypes());
                typeList.addAll(tableBuffer.getTableDefine(tableName).getColTypes());
                colNameList.addAll(tableBuffer.getTableDefine(tableName).getColNames());
            }
            while (from.hasNext()) {
                List<byte[]> record = from.next();
                List<Object> valueList = DataDump.loadAll(types, record);
                if (whereExpression != null) {
                    whereExpression.evalAll(valueList, typeList,colNameList, names, expressions, res);
                }else {
                    Expression.nullEvalAll(valueList, typeList,colNameList, names, expressions, res);
                }
            }
        }

        return String.join("\n", res);
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
                    indexUseType = DeleteExec.IndexUseType.OR_ALL;
                    for (String columnName : allUsedColumnNameSet) {
                        if (!haveIndexColumnName.contains(columnName)) {
                            isSimple = false;
                            indexUseType = DeleteExec.IndexUseType.NONE;
                            break;
                        }
                    }
                }
                case AND -> {
                    indexUseType = DeleteExec.IndexUseType.AND_ONE;
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
        if (whereExpression == null) {
            return new Pair<>(new ArrayList<>(), new ArrayList<>());
        }
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

    private void addPrintHead(List<String> res, List<String> names) {
        res.add(String.join(",", names));
    }

    public SelectExec(List<Expression> expressions, List<String> names, List<String> tableName, Expression whereExpression) {
        this.expressions = expressions;
        this.names = names;
        this.tableNames = tableName;
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
