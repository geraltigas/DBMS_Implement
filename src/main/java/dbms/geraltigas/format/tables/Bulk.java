package dbms.geraltigas.format.tables;

import dbms.geraltigas.bean.ApplicationContextUtils;
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
        byte[] bulkData = new byte[bulkNum * 4+4];
        diskManager.writeBytesAt(tableName, DiskManager.AccessType.BULK,null,DataDump.IntToBytes(bulkNum),0);
        int temp = bulkNum*4+4;
        for (int i = 4; i < temp; i+=4) {
            diskManager.writeBytesAt(tableName, DiskManager.AccessType.BULK,null,DataDump.IntToBytes(bulkContent.iterator().next()),i);
        }
    }

    public void initBulk() throws BlockException, IOException, DataDirException {
        ApplicationContextUtils.autowire(this);
        byte[] firstPageData = diskManager.readBytesAt(tableName, DiskManager.AccessType.BULK, null, 0, 4096);
        bulkNum = DataDump.BytesToInt(firstPageData, 0);
        byte[] bulkData = diskManager.readBytesAt(tableName, DiskManager.AccessType.BULK, null, 4, bulkNum*4);
        int temp = bulkNum*4+4;
        for (int i = 4; i < temp; i+=4) {
            bulkContent.add(DataDump.BytesToInt(bulkData, 0));
        }
    }
}
