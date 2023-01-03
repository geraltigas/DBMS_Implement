package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.CreateTableExec;
import dbms.geraltigas.dataccess.execplan.impl.ShowExec;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;


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
        if (createTable.getColumnDefinitions() == null) {
            ShowExec execPlan = new ShowExec(null,"No column definitions");
            ApplicationContextUtils.autowire(execPlan);
            execPlan.setThreadId(threadId);
            executeEngine.addExecPlan(execPlan);
            return execPlan.hashCode();
        }
        String[] colNames = createTable.getColumnDefinitions().stream().map(col -> col.getColumnName()).toArray(String[]::new);
        Set<String> colNameSet = new TreeSet<>();
        int colNum = colNames.length;
        for (String colName : colNames) {
            if (colNameSet.contains(colName)) {
                ShowExec execPlan = new ShowExec(null,"Duplicate column name");
                ApplicationContextUtils.autowire(execPlan);
                execPlan.setThreadId(threadId);
                executeEngine.addExecPlan(execPlan);
                return execPlan.hashCode();
            }
            colNameSet.add(colName);
        }
        if (colNameSet.size() != colNum) {
            ShowExec execPlan = new ShowExec(null,"Duplicate column name");
            ApplicationContextUtils.autowire(execPlan);
            execPlan.setThreadId(threadId);
            executeEngine.addExecPlan(execPlan);
            return execPlan.hashCode();
        }
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
