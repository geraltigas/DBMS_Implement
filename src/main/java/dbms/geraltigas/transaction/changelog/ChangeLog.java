package dbms.geraltigas.transaction.changelog;

import dbms.geraltigas.exception.BlockException;

import java.io.IOException;

public abstract class ChangeLog
{
    protected ChangeType changeType;

    public enum ChangeType
    {
        CREATE_FILE,
        PAGE_WRITE,
    }

    public void recover() throws BlockException, IOException {

    }
}
