package dbms.geraltigas.dataccess;

import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.tables.PageHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DiskManagerTest {


    @Autowired
    DiskManager diskManager;

    @Test
    void tableEndOffset() throws BlockException, DataDirException, IOException {
        assertEquals(13, diskManager.getTableHeader("student"));
    }

    @Test
    void readOneRecord() throws BlockException, DataDirException, IOException {
        byte[] data = diskManager.getOneRecord("test1",1, 0);
        data[0] = 0;
        data[1] = 10;
        diskManager.setOneRecord("test1", 1, 0, data);
        while (true){}
    }

    @Test
    void readPageHeader() throws BlockException, DataDirException, IOException {
        PageHeader pageHeader = diskManager.getPageHeader("test1", 1);
        pageHeader.setRecordNum(6);
        diskManager.setPageHeader("test1", 1, pageHeader);
        while (true) {}
    }
}