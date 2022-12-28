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
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.utils.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static dbms.geraltigas.buffer.BlockBuffer.BLOCK_SIZE;

public class DeleteExec implements ExecPlan { // TODO:  change to lock and index

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
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        List<String> res = new ArrayList<>();

        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

        int recordNum = SelectExec.SELECT_PAGE_NUMBER*BLOCK_SIZE/tableDefine.getRecordLength();
        long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,0);
        lockManager.lockWrite(tableHeaderId, threadId);
        TableHeader tableHeader = new TableHeader(diskManager.readPage(tableName, 0));
        int tableLength = tableHeader.getTableLength();
        int deleteNum = 0;

        Pair<List<String>,List<String>> pair = tableBuffer.getIndexNameAndIndexColumnNameList(tableName);
        List<String> indexNameList = pair.getFirst();
        List<String> indexColumnNameList = pair.getSecond();
        Set<String> indexColumNameSet = new TreeSet<>(indexColumnNameList);

        if (indexNameList.size() == 0) {
            for (int index = 0; index < tableLength; index++) {
                byte[] records = diskManager.readRecords(tableName, index+1);
                byte[] pageData = diskManager.readPage(tableName, index+1);
                PageHeader pageHeader = new PageHeader(pageData);
                if (records.length == 0) {
                    break;
                }
                byte[] record = new byte[tableDefine.getRecordLength()];
                int delTemp = 0;
                for (int i = 0; i < records.length/tableDefine.getRecordLength(); i+=1) {

                    System.arraycopy(records, i*tableDefine.getRecordLength(), record, 0, tableDefine.getRecordLength());
                    // judge the record is valid
                    if (record[0] == 1) {
                        // judge the record is valid
                        if (whereExpression != null) {
                            if (whereExpression.evalNoAlias(record, tableDefine)) {
                                record[0] = 0;
                                deleteNum++;
                                delTemp++;
                                diskManager.writeRecord(tableName,index+1,i,record,tableDefine.getRecordLength(),pageHeader);
                            }
                        }else {
                            record[0] = 0;
                            deleteNum++;
                            delTemp++;
                            diskManager.writeRecord(tableName,index+1,i,record,tableDefine.getRecordLength(),pageHeader);
                        }
                    }
                }
            }
        }else {
            List<Integer> pageIdList = new ArrayList<>();
            // we only need to support const value operation in delete
            Pair<List<String>,List<Pair<Expression.Op,Object>>> columnNameAndValueList = getColumnNameAndValueList(whereExpression);
            List<String> whereColumnNameList = columnNameAndValueList.getFirst();
            List<Pair<Expression.Op,Object>> valueList = columnNameAndValueList.getSecond();
            Set<String> whereColumnNameSet = new TreeSet<>(whereColumnNameList);

            // set and operation
            Set<String> haveIndexColumnName = new TreeSet<>();
            haveIndexColumnName.addAll(indexColumNameSet);
            haveIndexColumnName.retainAll(whereColumnNameSet);
            System.out.println(haveIndexColumnName);

        }

        res.add("Delete " + deleteNum + " records from table " + tableName);

        return String.join("\n", res);
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
