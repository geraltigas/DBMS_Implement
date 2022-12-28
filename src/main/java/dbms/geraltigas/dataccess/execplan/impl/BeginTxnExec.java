package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class BeginTxnExec implements ExecPlan {

    @Autowired
    TableBuffer tableBuffer;
    @Autowired
    DiskManager diskManager;
    @Autowired
    ExecuteEngine executeEngine;
    long threadId;
    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }
    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public long getThreadId() {
        return threadId;
    }


    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        executeEngine.beginTxn(threadId);
        return "Transaction begin";
    }
}
