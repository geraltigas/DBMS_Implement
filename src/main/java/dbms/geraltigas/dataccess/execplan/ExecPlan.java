package dbms.geraltigas.dataccess.execplan;

import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.exception.*;

import java.io.IOException;

public interface ExecPlan {
    long threadId = 0;
    void setThreadId(long threadId);
    long getThreadId();
    boolean getIsTxn();
    void setTxn(boolean txn, Executor executor);
    String execute(String dataPath) throws ThreadStopException,IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException;
}
