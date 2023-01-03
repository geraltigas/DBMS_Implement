package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.ShowExec;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;

public class ShowHandler implements Handler {

    ExecuteEngine executeEngine;
    private long threadId;

    @Override
    public int handle(Statement query) throws HandleException, DataTypeException, DropTypeException, ExpressionException {
        return 0;
    }

    public int handleShow(String var) throws HandleException, DataTypeException, DropTypeException, ExpressionException {
        ShowExec execPlan = new ShowExec(var,null);
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
