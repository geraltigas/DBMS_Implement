package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.RollbackTxnExec;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;

public class RollbackTxnHandler implements Handler {
    ExecuteEngine executeEngine;
    long threadId;
    @Override
    public int handle(Statement query) throws HandleException, DataTypeException, DropTypeException, ExpressionException {
        RollbackTxnExec execPlan = new RollbackTxnExec();
        ApplicationContextUtils.autowire(execPlan);
        execPlan.setThreadId(threadId);
        executeEngine.addExecPlan(execPlan);
        return execPlan.hashCode();
    }

    @Override
    public String getResault(int hash) {
        return executeEngine.getResult(hash);
    }

    @Override
    public void setDataAccesser(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
}
