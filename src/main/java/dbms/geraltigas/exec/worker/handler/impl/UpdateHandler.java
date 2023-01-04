package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;

public class UpdateHandler implements Handler {
    ExecuteEngine executeEngine;
    public UpdateHandler() {
    }

    @Override
    public void setDataAccesser(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public void setThreadId(long threadId) {

    }

    @Override
    public int handle(Statement query) {
        return 0;
    }

    @Override
    public String getResault(int hash) {
        return null;
    }
}

