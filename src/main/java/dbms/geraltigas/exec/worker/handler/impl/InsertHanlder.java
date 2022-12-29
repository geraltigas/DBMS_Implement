package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.InsertExec;
import dbms.geraltigas.exec.worker.handler.Handler;
import dbms.geraltigas.format.tables.TableDefine;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public class InsertHanlder implements Handler {

    ExecuteEngine executeEngine;
    long threadId = 0;
    @Autowired
    TableBuffer tableBuffer;
    public InsertHanlder() {
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
    public int handle(Statement query) {
        Insert insert = (Insert) query;
        String tableName = insert.getTable().getName();
        String[] colNames;
        if (insert.getColumns() == null) {
            TableDefine tableDefine = tableBuffer.getTableDefine(tableName);
            colNames = tableDefine.getColNames().toArray(new String[0]);
        }else {
            colNames = insert.getColumns()
                    .stream()
                    .map(col -> col.getColumnName())
                    .toArray(String[]::new);
        }
        List<List<Expression>> colValues = new ArrayList<>();
        List<SelectBody> values = ((SetOperationList) insert.getSelect().getSelectBody()).getSelects();
        for (SelectBody value : values) {
            List<Expression> expressions = (((ExpressionList) ((ValuesStatement)value).getExpressions()).getExpressions());
            for (Expression expression : expressions) {
                List<Expression> meta = ((RowConstructor) expression).getExprList().getExpressions();
                colValues.add(meta);
            }
        }
        InsertExec execPlan = new InsertExec(tableName, colNames, colValues);
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
