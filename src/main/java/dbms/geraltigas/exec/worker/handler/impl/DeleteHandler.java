package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecList;
import dbms.geraltigas.dataccess.execplan.impl.DeleteExec;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;

public class DeleteHandler implements Handler {

    ExecList execList;
    public DeleteHandler() {

    }

    @Override
    public void setDataAccesser(ExecList execList) {
        this.execList = execList;
    }

    @Override
    public int handle(Statement query) throws ExpressionException { // TODO: handle DELETE
        Delete delete = (Delete) query;
        String tableName = delete.getTable().getName();
        Expression where = delete.getWhere();
        dbms.geraltigas.expression.Expression whereExpression = SelectHandler.parseExpression(where);
        DeleteExec execPlan = new DeleteExec(tableName,whereExpression);
        ApplicationContextUtils.autowire(execPlan);
        execList.addExecPlan(execPlan);
        return execPlan.hashCode();
    }

    @Override
    public String getResault(int hash) {
        return execList.getResault(hash);
    }
}

