package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.dataccess.ExecList;
import dbms.geraltigas.exec.worker.handler.Handler;
import net.sf.jsqlparser.statement.Statement;

public class UpdateHandler implements Handler {
    ExecList execList;
    public UpdateHandler() {
    }

    @Override
    public void setDataAccesser(ExecList execList) {
        this.execList = execList;
    }

    @Override
    public void setThreadId(long threadId) {

    }

    @Override
    public int handle(Statement query) { // TODO: handle UPDATE
        return 0;
    }

    @Override
    public String getResault(int hash) {
        return null;
    }
}

