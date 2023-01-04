package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.*;
import dbms.geraltigas.transaction.LockManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class RollbackTxnExec implements ExecPlan {
    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;
    long threadId;
    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public long getThreadId() {
        return threadId;
    }
    @Override
    public boolean getIsTxn() {
        return isTxn;
    }
    boolean isTxn;
    Executor transactionExecutor;
    @Autowired
    ExecuteEngine executeEngine;
    @Autowired
    LockManager lockManager;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException, ThreadStopException {
        if (executeEngine.existTxn(threadId)) {
            executeEngine.rollbackTxn(threadId);
            lockManager.unlockAll(threadId);
            throw new ThreadStopException("Transaction rollbacked");
        }
        return null;
    }
}
