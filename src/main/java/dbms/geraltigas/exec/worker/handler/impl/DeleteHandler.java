package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.DeleteExec;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;

public class DeleteHandler implements Handler {

    long threadId = 0;

    ExecuteEngine executeEngine;
    public DeleteHandler() {

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
    public int handle(Statement query) throws ExpressionException { // TODO: handle DELETE
        Delete delete = (Delete) query;
        String tableName = delete.getTable().getName();
        Expression where = delete.getWhere();
        dbms.geraltigas.expression.Expression whereExpression = SelectHandler.parseExpression(where);
        DeleteExec execPlan = new DeleteExec(tableName,whereExpression);
        ApplicationContextUtils.autowire(execPlan);
        execPlan.setThreadId(threadId);
        executeEngine.addExecPlan(execPlan);
        return execPlan.hashCode();
    }

    @Override
    public String getResault(int hash) {
        return executeEngine.getResult(hash);
    }
}

