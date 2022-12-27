package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecList;
import dbms.geraltigas.dataccess.execplan.impl.ShowExec;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;

public class ShowHandler implements Handler {

    ExecList execList;
    private long threadId;

    @Override
    public int handle(Statement query) throws HandleException, DataTypeException, DropTypeException, ExpressionException {
        return 0;
    }

    public int handleShow(String var) throws HandleException, DataTypeException, DropTypeException, ExpressionException {
        ShowExec execPlan = new ShowExec(var);
        ApplicationContextUtils.autowire(execPlan);
        execPlan.setThreadId(threadId);
        execList.addExecPlan(execPlan);
        return execPlan.hashCode();
    }

    @Override
    public String getResault(int hash) {
        return execList.getResault(hash);
    }

    @Override
    public void setDataAccesser(ExecList execList) {
        this.execList = execList;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
}
