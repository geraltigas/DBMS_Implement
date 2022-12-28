package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.CreateTableExec;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.List;


public class CreateTableHandler implements Handler {

    ExecuteEngine executeEngine;
    long threadId = 0;

    @Override
    public void setDataAccesser(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public CreateTableHandler() {
    }

    @Override
    public int handle(Statement query) throws DataTypeException {
        CreateTable createTable = (CreateTable) query;
        String tableName = createTable.getTable().getName();
        String[] colNames = createTable.getColumnDefinitions().stream().map(col -> col.getColumnName()).toArray(String[]::new);
        String[] colTypes = createTable.getColumnDefinitions().stream().map(col -> col.getColDataType().getDataType()).toArray(String[]::new);
        List<String>[] colAttrs = createTable.getColumnDefinitions().stream().map(col -> col.getColDataType().getArgumentsStringList()).toArray(List[]::new);
        CreateTableExec execPlan = new CreateTableExec(tableName, colNames, colTypes, colAttrs);
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
