package dbms.geraltigas.dataccess;

import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
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
}