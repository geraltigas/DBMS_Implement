package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class IndexHeaderChangeLog extends ChangeLog {
    String tableName;
    String indexName;
    IndexHeader oldIndexHeader;

    @Autowired
    DiskManager diskManager;

    public IndexHeaderChangeLog(String tableName, String indexName, IndexHeader oldIndexHeader) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.oldIndexHeader = oldIndexHeader;
        super.changeType = ChangeType.INDEX_HEADER;
        ApplicationContextUtils.autowire(this);
    }

    @Override
    public void recover() throws BlockException, DataDirException, IOException {
        diskManager.setIndexHeader(tableName, indexName, oldIndexHeader);
        System.out.println("[ChangeLog] rollback index header :" + tableName + ", \nindexName: " + indexName + " ");
    }
}
