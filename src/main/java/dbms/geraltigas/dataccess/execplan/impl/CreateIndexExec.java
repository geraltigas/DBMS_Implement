package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.transaction.LockManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CreateIndexExec implements ExecPlan { // TODO: implement this
    String indexName;
    String tableName;
    String columnName;
    @Autowired
    LockManager lockManager;
    @Autowired
    DiskManager diskManager;

    private long threadId;
    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    public CreateIndexExec(String indexName, String tableName, String columnName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public String execute(String dataPath) throws IOException, BlockException, DataDirException {
        Path dataDir = Paths.get(dataPath);
        if (!dataDir.toFile().exists()) {
            return "Data path not exists";
        }

        Path indexsDir = dataDir.resolve("indexes");
        if (!indexsDir.toFile().exists()) {
            indexsDir.toFile().mkdirs();
        }

        Path indexDir = indexsDir.resolve(tableName);
        if (!indexDir.toFile().exists()) {
            indexDir.toFile().mkdirs();
        }

        Path indexFile = indexDir.resolve(indexName+"["+tableName+"("+columnName+")"+"]"+".idx");
        if (indexFile.toFile().exists()) {
            return "Index already exists";
        }

        boolean succ = indexFile.toFile().createNewFile();
        if (!succ) {
            return "Create index " + indexName + " on " + tableName+"."+columnName+ " failed";
        }

        // TODO: here should generic index data and write it to index file
        long headerId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
        lockManager.lockWrite(headerId, threadId);

        IndexHeader indexHeader = diskManager.getIndexHeader(tableName, indexName);

        return "Create index " + indexName + " on " + tableName+"."+columnName+ " success";
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
