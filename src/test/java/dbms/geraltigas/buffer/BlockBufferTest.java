package dbms.geraltigas.buffer;

import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class BlockBufferTest {

    @Autowired
    BlockBuffer blockBuffer;

    @Test
    void getHashCode() {
//        String tableName = "test";
//        int blockId = 1;
//        String isIndex = "false";
//        BlockBuffer blockBuffer = new BlockBuffer();
//        int hashCode = blockBuffer.GetHashCode(tableName, isIndex, blockId);
//        System.out.println(hashCode);
//        String tableName2 = "test";
//        isIndex = "true";
//        int hashCode2 = blockBuffer.GetHashCode(tableName2, isIndex, blockId);
//        int hashCode3 = blockBuffer.GetHashCode(tableName2, isIndex, blockId);
//        System.out.println(hashCode2);
//        assertEquals(hashCode2, hashCode3);
    }

    @Test
    void getBlockFromDisk() {
        try {
            BlockBuffer.Page page = blockBuffer.getPage("test", DiskManager.AccessType.INDEX, "index1", 0);
            System.out.println(page.data);
        } catch (BlockException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}