package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.CreateIndexExec;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;

public class CreateIndexHandler implements Handler {
    ExecuteEngine executeEngine;
    long threadId = 0;

    @Override
    public void setDataAccesser(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public int handle(Statement query) throws HandleException { // TODO: handle CREATE INDEX
        CreateIndex createIndex = (CreateIndex) query;
        String indexName = createIndex.getIndex().getName();
        String tableName = createIndex.getTable().getName();
        if (createIndex.getIndex().getColumnsNames().size() != 1) {
            throw new HandleException("Create index only support one column");
        }
        String columnName = createIndex.getIndex().getColumnsNames().get(0);
        CreateIndexExec createIndexExec = new CreateIndexExec(indexName, tableName, columnName);
        createIndexExec.setThreadId(threadId);
        executeEngine.addExecPlan(createIndexExec);
        return createIndexExec.hashCode();
    }

    @Override
    public String getResault(int hash) {
        return executeEngine.getResult(hash);
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
}

