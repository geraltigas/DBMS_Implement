package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class IndexChangeLog extends ChangeLog {
    String tableName;
    int pageIndex;
    String indexName;
    int indexDataIndex;
    int indexDataLength;

    byte[] oldIndexData;
    @Autowired
    DiskManager diskManager;
    public IndexChangeLog(String tableName, int pageIndex, String indexName, int indexDataIndex, int indexDataLength, byte[] oldIndexData) {
        changeType = ChangeType.INDEX;
        this.tableName = tableName;
        this.pageIndex = pageIndex;
        this.indexName = indexName;
        this.indexDataIndex = indexDataIndex;
        this.indexDataLength = indexDataLength;
        this.oldIndexData = oldIndexData;
        ApplicationContextUtils.autowire(this);
    }
    @Override
    public void recover() throws BlockException, IOException {
        diskManager.setOneIndexData(tableName, pageIndex, indexName, indexDataIndex, indexDataLength, oldIndexData);
    }
}
