package dbms.geraltigas.buffer;

import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.utils.Printer;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static dbms.geraltigas.utils.Printer.DEBUG;

@Component
public class PageBuffer {
    public static final int BLOCK_SIZE = 4096;
    private static final int BLOCK_COUNT = 4096;

    private ReentrantLock mutex = new ReentrantLock();
    private Page[] pageArrayBuffer;

    private ReentrantLock changedSetMutex = new ReentrantLock();
    private Deque<Page> changedPageSet = new ConcurrentLinkedDeque<>();
    @Autowired
    ExecuteEngine executeEngine;

    @Autowired
    ExecutorService executorService;

    public Page[] getPageArrayBuffer() {
        return pageArrayBuffer;
    }

    @PostConstruct
    public void init() {
        executorService.submit(() -> {
            Printer.print("Starting to data persistence", "DataPersistence");
            while (true) {
                changedSetMutex.lock();
                try {
                    if (changedPageSet.size() > 0) {
                        Page page = changedPageSet.poll();
                        if (page != null) {
//                            Page now = pageArrayBuffer[getHash]
                            Page now = pageArrayBuffer[getHashCode(page.tableName,page.type,page.appendPath,page.blockId)];
                            if (now != null && Objects.equals(now.tableName, page.tableName) && now.type == page.type && Objects.equals(now.appendPath, page.appendPath) && now.blockId == page.blockId) {
                                now.isWrited = false;
                            }
                        }
                        // write to disk
                        setBlockToDisk(page.tableName, page.type, page.appendPath, page.blockId, page.data);
                        Printer.print("[DataPersistence] Write page " + page.tableName + " " + page.type + " " + page.appendPath + " " + page.blockId + " to disk","Info");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    changedSetMutex.unlock();
                }
            }
        });
    }

    public void FlushPages(ArrayList<Page> pages) {
        mutex.lock();
        DEBUG("Get diskManager Lock");
        for (Page page: pages){
            setPage(page.tableName, page.type,page.appendPath, page.blockId,page,true);
        }
        mutex.unlock();
    }

     public void FlushIntoDisk() {
        mutex.lock();
         DEBUG("Get diskManager Lock");
//        for (int i = 0 ; i < changedPageSet.size();i++) {
//            Page page = changedPageSet.poll();
//            if (page == null) {
//                continue;
//            }
//            setBlockToDisk(page.tableName, page.type, page.appendPath, page.blockId, page.data);
//        }
//        // TODO : to be test
//        for (int i = 0; i < BLOCK_COUNT; i++) {
//            if (pageArrayBuffer[i] != null && pageArrayBuffer[i].blockId != -1 && pageArrayBuffer[i].tableName != null) {
//                pageArrayBuffer[i].isWrited = false;
//                // write to disk
//                setBlockToDisk(pageArrayBuffer[i].tableName, pageArrayBuffer[i].type, pageArrayBuffer[i].appendPath, pageArrayBuffer[i].blockId, pageArrayBuffer[i].data);
//            }
//        }
        mutex.unlock();
    }

    public void deleteTable(String tableName) {
        mutex.lock();
        DEBUG("Get diskManager Lock");
        for (int i = 0; i < this.pageArrayBuffer.length;i++) {
            Page page = this.pageArrayBuffer[i];
            if (page != null && page.tableName != null) {
                if (page.tableName.equals(tableName)) {
                    this.pageArrayBuffer[i] = null;
                }
            }
        }
        for (int i = 0; i < this.changedPageSet.size();i++) {
            Page page = this.changedPageSet.poll();
            if (page != null && page.tableName != null) {
                if (page.tableName.equals(tableName)) {
                    continue;
                }
            }
            this.changedPageSet.add(page);
        }
        mutex.unlock();
    }

    public Deque<Page> getChangedPageList() {
        return changedPageSet;
    }

    public void deleteRelatedPage(String tableName, DiskManager.AccessType table, Object o) {
        mutex.lock();
        DEBUG("Get diskManager Lock");
        for (int i = 0; i < this.pageArrayBuffer.length;i++) {
            Page page = this.pageArrayBuffer[i];
            if (page != null && page.tableName != null) {
                if (page.tableName.equals(tableName)) {
                    if (table == DiskManager.AccessType.TABLE) {
                        this.pageArrayBuffer[i] = null;
                    } else if (table == DiskManager.AccessType.INDEX) {
                        if (page.appendPath.equals(o)) {
                            this.pageArrayBuffer[i] = null;
                        }
                    }
                }
            }
        }
        for (int i = 0; i < this.changedPageSet.size();i++) {
            Page page = this.changedPageSet.poll();
            if (page != null && page.tableName != null) {
                if (page.tableName.equals(tableName)) {
                    if (table == DiskManager.AccessType.TABLE) {
                        this.changedPageSet.add(page);
                    } else if (table == DiskManager.AccessType.INDEX) {
                        if (page.appendPath.equals(o)) {
                            this.changedPageSet.add(page);
                        }
                    }
                }
            }
        }
        mutex.unlock();
    }

    public class Page implements Comparable<Page> {
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

        @Override
        public int compareTo(Page o) {
            String str1 = this.tableName + this.type + this.appendPath + this.blockId;
            String str2 = o.tableName + o.type + o.appendPath + o.blockId;
            return str1.compareTo(str2);
        }
    }


    public PageBuffer() {
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

    public Page getPage(String tableName, DiskManager.AccessType type, String appendPath, int blockId) {
        mutex.lock();
        DEBUG("Get diskManager Lock");
        int hashCode = getHashCode(tableName, type, appendPath, blockId);
        DEBUG("table name: " + tableName + " type: " + type + " appendPath: " + appendPath + " blockId: " + blockId + " hashcode: " + hashCode);
        if (pageArrayBuffer[hashCode] == null) {
            DEBUG("page is null");
            changedSetMutex.lock();
            for (Page page : changedPageSet) {
                if (page.tableName.equals(tableName) && page.type == type && Objects.equals(page.appendPath, appendPath) && page.blockId == blockId) {
                    pageArrayBuffer[hashCode] = page;
                    mutex.unlock();
                    changedPageSet.remove(page);
                    changedSetMutex.unlock();
                    return page;
                }
            }
            changedSetMutex.unlock();
            try {
                pageArrayBuffer[hashCode] = new Page(tableName,type, appendPath ,blockId, getBlockFromDisk(tableName, type, appendPath, blockId));
            } catch (BlockException e) {
                throw new RuntimeException(e);
            }
        }
        Page page = pageArrayBuffer[hashCode];
        if (page.blockId != blockId || !Objects.equals(page.tableName, tableName) || page.type != type || !Objects.equals(page.appendPath, appendPath)) {
            if (page.isWrited) {
                DEBUG("page is writed");
                changedSetMutex.lock();
                changedPageSet.add(page);
                DEBUG("add page to changedPageSet");
                for (Page temp : changedPageSet) {
                    DEBUG("TEST ONE");
                    DEBUG("table name: " + temp.tableName + " type: " + temp.type + " appendPath: " + temp.appendPath + " blockId: " + temp.blockId);
                    if (temp.tableName.equals(tableName) && temp.type == type && Objects.equals(temp.appendPath, appendPath) && temp.blockId == blockId) {
                        DEBUG("page is in changedPageSet");
                        pageArrayBuffer[hashCode] = temp;
                        changedSetMutex.unlock();
                        mutex.unlock();
                        return temp;
                    }
                }
                changedSetMutex.unlock();
                DEBUG("page is not in changedPageSet");
                Page temp = null;
                try {
                    temp = new Page(tableName,type,appendPath,blockId, getBlockFromDisk(tableName, type, appendPath, blockId));
                } catch (BlockException e) {
                    throw new RuntimeException(e);
                }
                pageArrayBuffer[hashCode] = temp;
                mutex.unlock();
                return temp;
            }else {
                changedSetMutex.lock();
                DEBUG("page is not writed");
                for (Page temp : changedPageSet) {
                    if (temp.tableName.equals(tableName) && temp.type == type && Objects.equals(temp.appendPath, appendPath) && temp.blockId == blockId) {
                        pageArrayBuffer[hashCode] = temp;
                        changedSetMutex.unlock();
                        mutex.unlock();
                        return temp;
                    }
                }
                changedSetMutex.unlock();
                Page temp = null;
                try {
                    temp = new Page(tableName,type,appendPath,blockId, getBlockFromDisk(tableName,type,appendPath, blockId));
                } catch (BlockException e) {
                    throw new RuntimeException(e);
                }
                pageArrayBuffer[hashCode] = temp;
                mutex.unlock();
                return temp;
            }
        }
        mutex.unlock();
        return pageArrayBuffer[hashCode];
    }

     public void setPage(String tableName, DiskManager.AccessType type, String appendPath, int blockId, Page page, boolean isWrited) {
        int hashCode = getHashCode(tableName, type,appendPath, blockId);
        page.isWrited = isWrited;
        if (pageArrayBuffer[hashCode] == null) {
            pageArrayBuffer[hashCode] = page;
        }else {
            if (pageArrayBuffer[hashCode].blockId != blockId || !Objects.equals(pageArrayBuffer[hashCode].tableName, tableName) || pageArrayBuffer[hashCode].type != type || !Objects.equals(pageArrayBuffer[hashCode].appendPath, appendPath)) {
                if (pageArrayBuffer[hashCode].isWrited) {
                    changedSetMutex.lock();
                    changedPageSet.add(pageArrayBuffer[hashCode]);
                    changedSetMutex.unlock();
                }
                pageArrayBuffer[hashCode] = page;
            }else {
                pageArrayBuffer[hashCode] = page;
            }
            changedPageSet.add(page);
        }
    }

    private byte[] getBlockFromDisk(String tableName, DiskManager.AccessType type, String appendPath, int blockId) {
        long fromOffset = blockId * BLOCK_SIZE;
        long toOffset = fromOffset + BLOCK_SIZE;

        if (type == DiskManager.AccessType.INDEX) {
            Path indexPath = Paths.get(executeEngine.getDateDir(), "/indexes/"+tableName);
            // for in indexpath dir
            String[] dirList = indexPath.toFile().list();
            String finalAppendPath = appendPath;
            String[] filted = Arrays.stream(Objects.requireNonNull(dirList)).filter(s -> s.contains(finalAppendPath)).toArray(String[]::new);
            if (filted.length == 0) {
                try {
                    throw new BlockException("index file not found");
                } catch (BlockException e) {
                    throw new RuntimeException(e);
                }
            }
            appendPath = filted[0];
        }

        Path path;
        switch (type) {
            case TABLE -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".tbl");
            case INDEX -> path = Paths.get(executeEngine.getDateDir()+ "/indexes/" +tableName+"/"+ appendPath);
            case BULK -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".bulk");
            default -> {
                try {
                    throw new BlockException("Invalid access type");
                } catch (BlockException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        File file = path.toFile();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        byte[] data;
        try {
            randomAccessFile.seek(fromOffset);
            data = new byte[BLOCK_SIZE];
            randomAccessFile.read(data,0, (int) (toOffset - fromOffset));
            randomAccessFile.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }

    private boolean setBlockToDisk(String tableName, DiskManager.AccessType type, String appendPath, int blockId, byte[] data) {
        long fromOffset = blockId * BLOCK_SIZE;
        long toOffset = fromOffset + BLOCK_SIZE;
        Path path = null;
        if (type == DiskManager.AccessType.INDEX) {
            Path indexPath = Paths.get(executeEngine.getDateDir(), "/indexes/"+tableName);
            // for in indexpath dir
            String[] dirList = indexPath.toFile().list();
            String finalAppendPath = appendPath;
            String[] filted = Arrays.stream(Objects.requireNonNull(dirList)).filter(s -> s.contains(finalAppendPath)).toArray(String[]::new);
            if (filted.length == 0) {
                try {
                    throw new BlockException("index file not found");
                } catch (BlockException e) {
                    throw new RuntimeException(e);
                }
            }
            appendPath = filted[0];
        }
        switch (type) {
            case TABLE -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".tbl");
            case INDEX -> path = Paths.get(executeEngine.getDateDir()+ "/indexes/" +tableName+"/"+ appendPath);
            case BULK -> path = Paths.get(executeEngine.getDateDir()+ "/tables/" + tableName + ".bulk");
            default -> {
                try {
                    throw new BlockException("Invalid access type");
                } catch (BlockException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        File file = path.toFile();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            randomAccessFile.seek(fromOffset);
            randomAccessFile.write(data);
            randomAccessFile.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

}
