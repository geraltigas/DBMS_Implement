package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class TableHeaderChangeLog extends ChangeLog {
    String tableName;
    TableHeader oldTableHeader;

    @Autowired
    DiskManager diskManager;

    public TableHeaderChangeLog(String tableName, TableHeader oldTableHeader) {
        this.tableName = tableName;
        this.oldTableHeader = oldTableHeader;
        super.changeType = ChangeType.TABLE_HEADER;
    }

    @Override
    public void recover() throws BlockException, IOException {
        diskManager.setTableHeader(tableName, oldTableHeader);
    }
}
