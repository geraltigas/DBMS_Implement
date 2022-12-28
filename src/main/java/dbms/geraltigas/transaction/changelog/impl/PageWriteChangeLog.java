package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class PageWriteChangeLog extends ChangeLog {
    private String tableName;
    private DiskManager.AccessType accessType;
    private String appendName;
    private int pageId;
    private byte[] rawData;
    private int offset;
    @Autowired
    DiskManager diskManager;
    public PageWriteChangeLog(String tableName, DiskManager.AccessType accessType, String appendName, int pageId) {
        super.changeType = ChangeType.PAGE_WRITE;
        this.tableName = tableName;
        this.accessType = accessType;
        this.appendName = appendName;
        this.pageId = pageId;
        ApplicationContextUtils.autowire(this);
    }

    @Override
    public void recover() throws BlockException, IOException {
        diskManager.writeBytesAt(tableName,accessType,appendName,rawData,offset);
    }
}
