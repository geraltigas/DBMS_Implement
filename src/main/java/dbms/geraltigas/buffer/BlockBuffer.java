package dbms.geraltigas.buffer;

import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.exception.BlockException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Objects;

@Component
public class BlockBuffer {
    public static int BLOCK_SIZE = 4096;
    private static int BLOCK_COUNT = 4096;

    synchronized public void FlushPages(ArrayList<Page> pages) throws BlockException, IOException {
        for (Page page : pages) {
            this.setPage(page.tableName,page.type,page.appendPath, page.blockId, page,true);
        }
    }

    synchronized public void FlushIntoDisk() throws BlockException {
        for (Page page : this.pageArrayBuffer) {
            if (page != null && page.isWrited) {
                try {
                    writeBlockToDisk(page.tableName, page.type, page.blockId, page.data);
                    setPage(page.tableName, page.type, page.appendPath, page.blockId, page, false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class Page {
        public String tableName = null;
        public DiskManager.AccessType type = null;
        public String appendPath = null;
        public boolean isWrited = false;
        public int blockId = -1;
        public byte[] data;
        public Page() {
            data = new byte[4096];
        }


        public Page(String tableName, DiskManager.AccessType type, String appendPath, int blockId, byte[] data) throws BlockException {
            if (data.length != 4096) {
                throw new BlockException("data length must be 4096");
            }
            this.data = data;
            this.tableName = tableName;
            this.type = type;
            this.blockId = blockId;
            this.appendPath = appendPath;
        }

        public void writeBytes(byte[] data, int blockOffset, int len) {
            System.arraycopy(data, 0, this.data, blockOffset, len);
        }

    }
    private Page[] pageArrayBuffer;
    private LinkedList<Page> changedPageList;
    @Autowired
    ExecuteEngine executeEngine;

    public BlockBuffer() {
        pageArrayBuffer = new Page[BLOCK_COUNT];
        for (int i = 0; i < 100; i++) {
            pageArrayBuffer[i] = new Page();
        }
    }

     private int getHashCode(String tableName, DiskManager.AccessType type, String appendPath, int blockId) {
        int hashCode = switch (type) {
            case TABLE -> (tableName.hashCode() + blockId) % BLOCK_COUNT;
            case INDEX -> (tableName.hashCode() + type.toString().hashCode() + appendPath.hashCode() + blockId) % BLOCK_COUNT;
            case BULK -> (tableName.hashCode() + type.toString().hashCode() + blockId) % BLOCK_COUNT;
        };
        if (hashCode < 0) {
            hashCode += BLOCK_COUNT;
        }
        return hashCode;
    }

    synchronized public Page getPage(String tableName, DiskManager.AccessType type, String appendPath, int blockId) throws BlockException, IOException {
        int hashCode = getHashCode(tableName, type, appendPath, blockId);
        if (pageArrayBuffer[hashCode] == null) {
            pageArrayBuffer[hashCode] = new Page(tableName,type, appendPath ,blockId, getBlockFromDisk(tableName, type, appendPath, blockId));
        }
        Page page = pageArrayBuffer[hashCode];
        if (page.blockId != blockId || !Objects.equals(page.tableName, tableName) || page.type != type || !Objects.equals(page.appendPath, appendPath)) {
            if (page.isWrited) {
                writeBlockToDisk(page.tableName, page.type, page.blockId, page.data);
                Page temp = new Page(tableName,type,appendPath,blockId, getBlockFromDisk(tableName, type, appendPath, blockId));
                pageArrayBuffer[hashCode] = temp;
                return temp;
            }else {
                Page temp = new Page(tableName,type,appendPath,blockId, getBlockFromDisk(tableName,type,appendPath, blockId));
                pageArrayBuffer[hashCode] = temp;
                return temp;
            }
        }
        return pageArrayBuffer[hashCode];
    }

    synchronized public void setPage(String tableName, DiskManager.AccessType type, String appendPath, int blockId, Page page, boolean isWrited) throws IOException {
        int hashCode = getHashCode(tableName, type,appendPath, blockId);
        page.isWrited = isWrited;
        pageArrayBuffer[hashCode] = page;
    }

    public byte[] getBlockFromDisk(String tableName, DiskManager.AccessType type, String appendPath, int blockId) throws BlockException, IOException {
        long fromOffset = blockId * BLOCK_SIZE;
        long toOffset = fromOffset + BLOCK_SIZE;
        Path path;
        switch (type) {
            case TABLE -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".tbl");
            case INDEX -> path = Paths.get(executeEngine.getDateDir()+ "/indexes/" +tableName+"/"+ appendPath);
            case BULK -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".bulk");
            default -> throw new BlockException("Invalid access type");
        }
        File file = path.toFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(fromOffset);
        byte[] data = new byte[BLOCK_SIZE];
        randomAccessFile.read(data,0, (int) (toOffset - fromOffset));
        return data;
    }

    public boolean writeBlockToDisk(String tableName, DiskManager.AccessType type, int blockId, byte[] data) throws IOException, BlockException {
        long fromOffset = blockId * BLOCK_SIZE;
        long toOffset = fromOffset + BLOCK_SIZE;
        Path path = null;
        switch (type) {
            case TABLE -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".tbl");
            case INDEX -> path = Paths.get(executeEngine.getDateDir()+ "/indexes/" +tableName+"/"+ type + ".idx");
            case BULK -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".bulk");
            default -> throw new BlockException("Invalid access type");
        }
        File file = path.toFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(fromOffset);
        randomAccessFile.write(data);
        return true;
    }

}
