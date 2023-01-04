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
import dbms.geraltigas.utils.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;

import static dbms.geraltigas.expression.Expression.evalAliasExpression;

public class SelectExec implements ExecPlan {
    List<Expression> expressions;
    List<String> names;
    List<String> tableNames;
    Expression whereExpression;

    private long threadId;

    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    @Autowired
    TableBuffer tableBuffer;

    boolean ambiguous = false;

    @Autowired
    DiskManager diskManager;

    @Autowired
    LockManager lockManager;

    DeleteExec.IndexUseType indexUseType = DeleteExec.IndexUseType.NONE;

    @Override
    public String execute(String dataPath) throws BlockException, DataDirException, IOException, DataTypeException {
        List<String> res = new ArrayList<>();
        addPrintHead(res,names);
        boolean useHashJoin = false;

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

            Printer.print("indexUseType: " + indexUseType,threadId);

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
                        PageHeader pageHeader = diskManager.getPageHeader(tableName, pageIndex);
                        int recordNumT = pageHeader.getRecordNum();
                        for (int recordIndex = 0 ; recordIndex < recordNumT;recordIndex++) {
                            byte[] record = diskManager.getOneRecord(tableName, pageIndex, recordIndex);
                            if (record[0] == 1) {
                                // judge the record is valid
                                try {
                                    if (whereExpression != null) {
                                        whereExpression.eval(record, tableDefine, names, expressions, res);
                                    }else {
                                        Expression.nullEval(record, tableDefine, names, expressions, res);
                                    }
                                }catch (DataTypeException e) {
                                    if (isTxn) lockManager.unlockAll(threadId);
                                    return e.getMessage();
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
                    for (Integer pageIndex : pageIndexSet) {
                        long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageIndex);
                        lockManager.lockRead(pageId,threadId);
                        PageHeader pageHeader = diskManager.getPageHeader(tableName, pageIndex);
                        int recordNumT = pageHeader.getRecordNum();
                        for (int recordIndex = 0 ; recordIndex < recordNumT;recordIndex++) {
                            byte[] record = diskManager.getOneRecord(tableName, pageIndex, recordIndex);
                            if (record[0] == 1) {
                                // judge the record is valid
                                try {
                                    if (whereExpression != null) {
                                        whereExpression.eval(record, tableDefine, names, expressions, res);
                                    }else {
                                        Expression.nullEval(record, tableDefine, names, expressions, res);
                                    }
                                }catch (DataTypeException e) {
                                    if (isTxn) lockManager.unlockAll(threadId);
                                    return e.getMessage();
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
                    Set<Integer> pageIndexSet = new TreeSet<>(pageIndexSetList.get(0));
                    for (Set<Integer> set : pageIndexSetList) {
                        pageIndexSet.retainAll(set);
                    }
                    for (Integer pageIndex : pageIndexSet) {
                        long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageIndex);
                        lockManager.lockRead(pageId,threadId);
                        PageHeader pageHeader = diskManager.getPageHeader(tableName, pageIndex);
                        int recordNumT = pageHeader.getRecordNum();
                        for (int recordIndex = 0 ; recordIndex < recordNumT;recordIndex++) {
                            byte[] record = diskManager.getOneRecord(tableName, pageIndex, recordIndex);
                            if (record[0] == 1) {
                                // judge the record is valid
                                try {
                                    if (whereExpression != null) {
                                        whereExpression.eval(record, tableDefine, names, expressions, res);
                                    }else {
                                        Expression.nullEval(record, tableDefine, names, expressions, res);
                                    }
                                }catch (DataTypeException e) {
                                    if (isTxn) lockManager.unlockAll(threadId);
                                    return e.getMessage();
                                }
                            }
                        }
                    }
                }
            }
        }else if (useHashJoin())
        {
            if (ambiguous) return "Ambiguous column name";
            Printer.print("using hash join",threadId);
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
                    PageHeader pageHeader = diskManager.getPageHeader(tableName, i+1);
                    int recordNumT = pageHeader.getRecordNum();
                    for (int j = 0;j < recordNumT;j++) {
                        byte[] record = diskManager.getOneRecord(tableName, i+1, j);
                        if (record[0] == 1) {
                            recordSet.add(record);
                        }
                    }
                }
                recordSetList.add(recordSet);
            }

            Map<Integer,List<List<Object>>> cache = new HashMap<>();

            String leftColumnName = whereExpression.getLeft().getName();
            String rightColumnName = whereExpression.getRight().getName();

            TableDefine firstTableDefine = tableBuffer.getTableDefine(tableNames.get(0));
            TableDefine secondTableDefine = tableBuffer.getTableDefine(tableNames.get(1));

            TableDefine leftTableDefine = null;
            TableDefine rightTableDefine = null;
            Set<byte[]> leftRecordSet = null;
            Set<byte[]> rightRecordSet = null;
            int leftIndex = -1;
            int rightIndex = -1;


            if (firstTableDefine.getColNames().contains(leftColumnName)) {
                leftTableDefine = firstTableDefine;
                rightTableDefine = secondTableDefine;
                leftRecordSet = recordSetList.get(0);
                rightRecordSet = recordSetList.get(1);
            }else {
                leftTableDefine = secondTableDefine;
                rightTableDefine = firstTableDefine;
                leftRecordSet = recordSetList.get(1);
                rightRecordSet = recordSetList.get(0);
            }

            List<String> nameList = new ArrayList<>();
            nameList.addAll(leftTableDefine.getColNames());
            nameList.addAll(rightTableDefine.getColNames());
            List<TableDefine.Type> types = new ArrayList<>();
            types.addAll(leftTableDefine.getColTypes());
            types.addAll(rightTableDefine.getColTypes());

            leftIndex = leftTableDefine.getColNames().indexOf(leftColumnName);
            rightIndex = rightTableDefine.getColNames().indexOf(rightColumnName);

            for (byte[] record : leftRecordSet) {
                List<Object> valueList = DataDump.load(leftTableDefine.getColTypes(), record,0);
                Object value = valueList.get(leftIndex);
                int hash = value.hashCode();
                if (cache.containsKey(hash)) {
                    cache.get(hash).add(valueList);
                }else {
                    List<List<Object>> valueListList = new ArrayList<>();
                    valueListList.add(valueList);
                    cache.put(hash,valueListList);
                }
            }

            for (byte[] record : rightRecordSet) {
                List<Object> valueList = DataDump.load(rightTableDefine.getColTypes(), record,0);
                Object value = valueList.get(rightIndex);
                int hash = value.hashCode();
                if (cache.containsKey(hash)) {
                    List<List<Object>> valueListList = cache.get(hash);
                    for (List<Object> valueList1 : valueListList) {

                        List<Object> values = new ArrayList<>();
                        values.addAll(valueList1);
                        values.addAll(valueList);

                        Map<String,Object> map = new HashMap<>();
                        Map<String, TableDefine.Type> typeMap = new HashMap<>();
                        for (int i = 0; i < nameList.size(); i++) {
                            map.put(nameList.get(i).trim(),values.get(i));
                            typeMap.put(nameList.get(i).trim(),types.get(i));
                        }

                        evalAliasExpression(typeMap,names,expressions,map);
                        String[] valueS = names.stream().map(item -> map.get(item.trim()).toString()).toArray(String[]::new);
                        for (int i = 0; i < valueS.length; i++) {
                            valueS[i] = String.format("%-10s",valueS[i]);
                        }
                        res.add("|"+String.join("|",valueS)+"|");
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
                    PageHeader pageHeader = diskManager.getPageHeader(tableName, i+1);
                    int recordNumT = pageHeader.getRecordNum();
                    for (int j = 0;j < recordNumT;j++) {
                        byte[] record = diskManager.getOneRecord(tableName, i+1, j);
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
            Set<String> colNameSet = new TreeSet<>();
            for (String tableName : tableNames) {
                types.add(tableBuffer.getTableDefine(tableName).getColTypes());
                typeList.addAll(tableBuffer.getTableDefine(tableName).getColTypes());
                colNameList.addAll(tableBuffer.getTableDefine(tableName).getColNames());
                colNameSet.addAll(tableBuffer.getTableDefine(tableName).getColNames());
            }
            if (colNameSet.size() != colNameList.size()) {
                if (isTxn) lockManager.unlockAll(threadId);
                return "Ambiguous column name";
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

        if(!isTxn) {
            lockManager.unlockAll(threadId);
        }

        res.add("Total records: " + (res.size()-1));

        return String.join("\n", res);
    }
    @Override
    public boolean getIsTxn() {
        return isTxn;
    }
    private boolean useHashJoin() throws IOException, DataTypeException {
        boolean isOk = true;
        if (whereExpression == null) {
            return false;
        }
        List<List<TableDefine.Type>> types = new ArrayList<>();
        List<TableDefine.Type> typeList = new ArrayList<>();
        List<String> colNameList = new ArrayList<>();
        Set<String> colNameSet = new TreeSet<>();
        for (String tableName : tableNames) {
            types.add(tableBuffer.getTableDefine(tableName).getColTypes());
            typeList.addAll(tableBuffer.getTableDefine(tableName).getColTypes());
            colNameList.addAll(tableBuffer.getTableDefine(tableName).getColNames());
            colNameSet.addAll(tableBuffer.getTableDefine(tableName).getColNames());
        }
        if (colNameSet.size() != colNameList.size()) {
            if (isTxn) lockManager.unlockAll(threadId);
            ambiguous = true;
            return true;
        }
        if (tableNames.size() != 2) {
            isOk = false;
        }
        if (whereExpression.getOp() != Expression.Op.EQUAL) {
            isOk = false;
        }
        if (whereExpression.getLeft().getOp() != Expression.Op.NULL || whereExpression.getRight().getOp() != Expression.Op.NULL) {
            isOk = false;
        }
        String leftColumnName = whereExpression.getLeft().getName();
        String rightColumnName = whereExpression.getRight().getName();
        TableDefine first = tableBuffer.getTableDefine(tableNames.get(0));
        TableDefine second = tableBuffer.getTableDefine(tableNames.get(1));
        TableDefine leftTableDefine = null;
        TableDefine rightTableDefine = null;
        String leftTableName = null;
        String rightTableName = null;

        if (first.getColNames().contains(leftColumnName)) {
            leftTableDefine = first;
            leftTableName = tableNames.get(0);
        }else {
            leftTableDefine = second;
            leftTableName = tableNames.get(1);
        }
        if (first.getColNames().contains(rightColumnName)) {
            rightTableDefine = first;
            rightTableName = tableNames.get(0);
        }else {
            rightTableDefine = second;
            rightTableName = tableNames.get(1);
        }
        if (
                (leftTableDefine.getColNames().contains(leftTableName) && leftTableDefine.getColNames().contains(rightTableName)) ||
                (rightTableDefine.getColNames().contains(leftTableName) && rightTableDefine.getColNames().contains(rightTableName))
        ) {
            isOk = false;
        }
        return isOk;
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

    private void addPrintHead(List<String> res, List<String> names) {
        for (int i = 0; i < names.size(); i++) {
            names.set(i, String.format("%-10s", names.get(i)));
        }
        res.add("|"+String.join("|",names)+"|");
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
