package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class CommitTxnExec implements ExecPlan {
    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;
    long threadId;
    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public long getThreadId() {
        return threadId;
    }

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        return null;
    }
}
