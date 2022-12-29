package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;

public class TablePageHeaderChangeLog extends ChangeLog {
    String tableName;
    int pageId;
    PageHeader oldPageHeader;

    public TablePageHeaderChangeLog(String tableName, int pageId, PageHeader oldPageHeader) {
        this.tableName = tableName;
        this.pageId = pageId;
        this.oldPageHeader = oldPageHeader;
        this.changeType = ChangeType.TABLE_PAGE_HEADER;
    }

    @Override
    public void recover() {

    }
}
