package dbms.geraltigas.format.tables;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class BulkTest {

    @Test
    void bulkTest() throws BlockException, DataDirException, IOException {
        Bulk bulk = new Bulk("test1");
        bulk.initBulk();
        bulk.addBulk(1);
        bulk.addBulk(2);
        bulk.addBulk(3);
        bulk.addBulk(4);
        bulk.addBulk(5);
        bulk.addBulk(6);
        bulk.addBulk(7);
        bulk.removeBulk(2);
        bulk.removeBulk(3);
        bulk.flush();
        while (true) {}
    }
}