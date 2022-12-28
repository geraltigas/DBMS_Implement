package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.expression.Expression;
import dbms.geraltigas.format.tables.TableDefine;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static dbms.geraltigas.buffer.BlockBuffer.BLOCK_SIZE;

public class SelectExec implements ExecPlan { // TODO:  change to lock and index

    List<Expression> expressions;
    private long threadId;


    public static final int SELECT_PAGE_NUMBER = 5;

    List<String> names;
    String tableName;
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

    @Override
    public String execute(String dataPath) throws BlockException, DataDirException, IOException {
        List<String> res = new ArrayList<>();

        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

        int recordNum = SELECT_PAGE_NUMBER*BLOCK_SIZE/tableDefine.getRecordLength();

        int index = 0;

        addPrintHead(res,names);

        int j = diskManager.getTableHeader(tableName).getTableLength();

        for (int i = 0; i < j; i++) {
            // read the records from the disk

            byte[] records = diskManager.readRecords(tableName, i+1);

            index+=records.length/tableDefine.getRecordLength();

            // judge the records is empty
            if (records.length == 0) {
                break;
            }

            byte[] record = new byte[tableDefine.getRecordLength()];

            for (int k = 0; k < records.length; k+=tableDefine.getRecordLength()) {
                System.arraycopy(records, k, record, 0, tableDefine.getRecordLength());
                // judge the record is valid
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

//        byte[] data = diskManager.ReadData(dataPath, tableName);

        return String.join("\n", res);
    }

    private void addPrintHead(List<String> res, List<String> names) {
        res.add(String.join(",", names));
    }

    public SelectExec(List<Expression> expressions, List<String> names, String tableName, Expression whereExpression) {
        this.expressions = expressions;
        this.names = names;
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
