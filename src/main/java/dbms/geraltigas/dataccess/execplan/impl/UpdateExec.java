package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;

import java.io.IOException;

public class UpdateExec implements ExecPlan { // TODO: implement this
    @Override
    public void setThreadId(long threadId) {

    }

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        return null;
    }
}
