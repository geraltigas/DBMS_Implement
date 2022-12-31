package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class RecordChangeLog extends ChangeLog {
    String tableName;
    int pageIndex;
    int recordIndex;
    byte[] oldRecord;
    @Autowired
    DiskManager diskManager;
    public RecordChangeLog(String tableName, int pageIndex, int recordIndex, byte[] oldRecord) {
        changeType = ChangeType.RECORD;
        this.tableName = tableName;
        this.pageIndex = pageIndex;
        this.recordIndex = recordIndex;
        this.oldRecord = oldRecord;
        ApplicationContextUtils.autowire(this);
    }
    @Override
    public void recover() throws BlockException, DataDirException, IOException {
        diskManager.setOneRecord(tableName, pageIndex, recordIndex, oldRecord);
    }
}
