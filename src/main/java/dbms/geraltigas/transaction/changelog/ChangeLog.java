package dbms.geraltigas.transaction.changelog;

import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;

import java.io.IOException;

public abstract class ChangeLog
{
    protected ChangeType changeType;

    public enum ChangeType
    {
        CREATE_FILE,
        RECORD,
        INDEX,
        TABLE_HEADER,
        INDEX_HEADER,
        INDEX_PAGE_HEADER,
        TABLE_PAGE_HEADER,
    }

    public void recover() throws BlockException, IOException, DataDirException {

    }
}
