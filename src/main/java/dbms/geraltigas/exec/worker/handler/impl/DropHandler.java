package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.DropExec;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

public class DropHandler implements Handler {

    ExecuteEngine executeEngine;

    long threadId = 0;
    public DropHandler() {
    }

    @Override
    public void setDataAccesser(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public int handle(Statement query) throws DropTypeException {
        Drop drop = (Drop) query;
        if (drop.getType().equalsIgnoreCase("TABLE")) {
            DropExec execPlan = new DropExec(drop.getName().getName());
            ApplicationContextUtils.autowire(execPlan);
            execPlan.setThreadId(threadId);
            executeEngine.addExecPlan(execPlan);
            return execPlan.hashCode();
        }else {
            throw new DropTypeException("Drop type not supported");
        }
    }

    @Override
    public String getResault(int hash) {
        return executeEngine.getResult(hash);
    }
}