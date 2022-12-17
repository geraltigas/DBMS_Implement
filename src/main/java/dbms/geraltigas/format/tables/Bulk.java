package dbms.geraltigas.format.tables;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.utils.DataDump;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;

@Setter
@Getter
public class Bulk {
    // usage:
    // flush: means write the data in memory to disk(asynchronously)
    // construct -> init -> add -> ... -> remove -> ... -> flush -> ...

    @Autowired
    DiskManager diskManager;

    private String tableName;
    private Set<Integer> bulkContent = new TreeSet<>();
    private int bulkNum;

    public Bulk(String tableName) throws BlockException, DataDirException, IOException {
        this.tableName = tableName;
    }

    public void addBulk(int bulk) {
        bulkContent.add(bulk);
    }

    public void removeBulk(int bulk) {
        bulkContent.remove(bulk);
    }

    public boolean containsBulk(int bulk) {
        return bulkContent.contains(bulk);
    }

    public void flush() throws BlockException, DataDirException, IOException {
        int bulkNum = bulkContent.size();
        int pageNum = (bulkNum+1)/1024+1;
        List<BlockBuffer.Page> pages = diskManager.readBulkPages(tableName);
        for (int i = pages.size(); i < pageNum; i++) {

        }
        diskManager.writeBulkPage(tableName, pages);
    }

    public void initBulk() throws BlockException, IOException, DataDirException {
        byte[] firstPageData = diskManager.readBytesAt(tableName, DiskManager.AccessType.BULK, null, 0, 4096);
        bulkNum = DataDump.BytesToInt(firstPageData, 0);

        for (int i = 0; i < bulkNum; i++) {
            int pageId = i/1024;
            int pageOffset = i%1024;
            int pageOffsetByte = pageOffset*4;
            int pageOffsetByteEnd = pageOffsetByte+4;
            int pageOffsetByteEndIndex = pageOffsetByteEnd/BlockBuffer.BLOCK_SIZE;
            int pageOffsetByteEndOffset = pageOffsetByteEnd%BlockBuffer.BLOCK_SIZE;
            if (pageOffsetByteEndIndex == 0) {
                bulkContent.add(DataDump.BytesToInt(pages.get(pageId).data,pageOffsetByte));
            } else {
                byte[] bytes = new byte[4];
                System.arraycopy(pages.get(pageId).data,pageOffsetByte,bytes,0,BlockBuffer.BLOCK_SIZE-pageOffsetByte);
                System.arraycopy(pages.get(pageId+1).data,0,bytes,BlockBuffer.BLOCK_SIZE-pageOffsetByte,pageOffsetByteEndOffset);
                bulkContent.add(DataDump.BytesToInt(bytes,0));
            }
        }
    }
}
