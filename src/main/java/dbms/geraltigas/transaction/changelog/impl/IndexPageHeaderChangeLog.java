package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.indexs.IndexPageHeader;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class IndexPageHeaderChangeLog extends ChangeLog {
    String tableName;
    int pageId;
    IndexPageHeader oldPageHeader;
    String indexName;

    @Autowired
    DiskManager diskManager;
    public IndexPageHeaderChangeLog(String tableName,String indexName, int pageId, IndexPageHeader oldPageHeader) {
        this.tableName = tableName;
        this.pageId = pageId;
        this.oldPageHeader = oldPageHeader;
        this.indexName = indexName;
        this.changeType = ChangeType.INDEX_PAGE_HEADER;
        ApplicationContextUtils.autowire(this);
    }

    @Override
    public void recover() throws BlockException, DataDirException, IOException {
        diskManager.setIndexPageHeader(tableName,indexName ,pageId, oldPageHeader);
    }
}
