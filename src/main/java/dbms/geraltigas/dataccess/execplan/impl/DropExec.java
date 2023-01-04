package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.PageBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DropExec implements ExecPlan {
    String tableName;
    private long threadId;
    boolean isTxn;
    Executor transactionExecutor;
    @Autowired
    PageBuffer pageBuffer;
    @Autowired
    LockManager lockManager;
    @Autowired
    DiskManager diskManager;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }
    public DropExec(String tableName) {
        this.tableName = tableName;
    }
    @Override
    public boolean getIsTxn() {
        return isTxn;
    }
    @Override
    public String execute(String dataPath) throws BlockException, DataDirException, IOException {
        List<String> res = new ArrayList<>();
        if (isTxn) {
            return "Dont support drop in txn";
        }
        Path path = Paths.get(dataPath + "/tables");
        if (path.toFile().exists()) {
            Path tablePath = path.resolve(tableName + ".tbl");
            pageBuffer.deleteTable(tableName);
            if (tablePath.toFile().exists()) {
                long tablePageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE, null, 0);
                lockManager.lockWrite(tablePageId, threadId);
                TableHeader tableHeader = diskManager.getTableHeader(tableName);
                int pageNum = tableHeader.getTableLength();
                for (int i = 0; i < pageNum; i++) {
                    long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE, null, i+1);
                    lockManager.lockWrite(pageId, threadId);
                }
                boolean succ = tablePath.toFile().delete();
                if (succ) {
                    res.add("Table file " + tableName + " dropped");
                } else {
                    lockManager.unlockAll(threadId);
                    return "Failed to drop table " + tableName;
                }
            } else {
                res.add("Table " + tableName + " not exists");
                return String.join(";\n", res);
            }
        }
        Path indexDir = Paths.get(dataPath + "/indexes/" + tableName);
        if (indexDir.toFile().exists()) {
            for (File i : Objects.requireNonNull(indexDir.toFile().listFiles())) {
                long indexHeadId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX, null, 0);
                lockManager.lockWrite(indexHeadId, threadId);
                String indexName = i.getName().split("\\[")[0];
                IndexHeader indexHeader = diskManager.getIndexHeader(tableName, indexName);
                int pageNum = indexHeader.getIndexDataPageNum();
                for (int j = 0; j < pageNum; j++) {
                    long pageId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX, indexName, j+1);
                    lockManager.lockWrite(pageId, threadId);
                }
                boolean succ = i.delete();
                if (!succ) {
                    lockManager.unlockAll(threadId);
                    return "Failed to drop index " + i.getName();
                }
                res.add("Index " + i.getName() + " deleted");
            }
            indexDir.toFile().delete();
            res.add("Index directory " + tableName + " deleted");
        }

        Path metaPath = Paths.get(dataPath + "/metadata/" + tableName + ".meta");
        if (metaPath.toFile().exists()) {
            boolean succ = metaPath.toFile().delete();
            if (succ) {
                res.add("Meta " + tableName + " deleted");
            } else {
                return "Failed to drop meta " + tableName;
            }
        }
        res.add("Table " + tableName + " dropped");
        lockManager.unlockAll(threadId);
        return String.join(";\n", res);
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
    @Override
    public long getThreadId() {
        return this.threadId;
    }
}
