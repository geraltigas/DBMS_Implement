package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.dataccess.ExecList;
import dbms.geraltigas.dataccess.execplan.impl.CreateIndexExec;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;

public class CreateIndexHandler implements Handler {
    ExecList execList;

    @Override
    public void setDataAccesser(ExecList execList) {
        this.execList = execList;
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
        execList.addExecPlan(createIndexExec);
        return createIndexExec.hashCode();
    }

    @Override
    public String getResault(int hash) {
        return execList.getResault(hash);
    }
}

