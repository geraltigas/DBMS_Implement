package dbms.geraltigas.buffer;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.tables.Bulk;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Set;

@SpringBootTest
class TableBufferTest {

    @Test
    void getTableBulk() throws BlockException, DataDirException, IOException {
        Bulk bulk = new Bulk("test1");
        ApplicationContextUtils.autowire(bulk);
        bulk.initBulk();
        Set<Integer> set = bulk.getBulkContent();
        System.out.println(set);
    }
}