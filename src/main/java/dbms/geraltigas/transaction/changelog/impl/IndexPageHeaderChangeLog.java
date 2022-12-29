package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;

public class IndexPageHeaderChangeLog extends ChangeLog {
    String tableName;
    int pageId;
    PageHeader oldPageHeader;

    public IndexPageHeaderChangeLog(String tableName, int pageId, PageHeader oldPageHeader) {
        this.tableName = tableName;
        this.pageId = pageId;
        this.oldPageHeader = oldPageHeader;
        this.changeType = ChangeType.INDEX_PAGE_HEADER;
    }

    @Override
    public void recover() {

    }
}
