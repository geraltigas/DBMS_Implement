package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.dataccess.DiskManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class PageWriteChangeLogTest {
    @Test
    void recover() {
        PageWriteChangeLog pageWriteChangeLog = new PageWriteChangeLog("test", DiskManager.AccessType.TABLE, "test", 1);
        assertNotNull(pageWriteChangeLog.diskManager);
    }

}