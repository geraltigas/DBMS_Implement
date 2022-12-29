package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;

public class TableHeaderChangeLog extends ChangeLog {
    String tableName;
    TableHeader oldTableHeader;

    public TableHeaderChangeLog(String tableName, TableHeader oldTableHeader) {
        this.tableName = tableName;
        this.oldTableHeader = oldTableHeader;
        super.changeType = ChangeType.TABLE_HEADER;
    }

    @Override
    public void recover() {

    }
}
