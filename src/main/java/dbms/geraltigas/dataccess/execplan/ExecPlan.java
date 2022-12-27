package dbms.geraltigas.dataccess.execplan;

import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;

import java.io.IOException;

public interface ExecPlan {
    long threadId = 0;
    void setThreadId(long threadId);
    String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException;
}
