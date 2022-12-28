package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.indexs.IndexPageHeader;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.transaction.changelog.impl.CreateFileChangeLog;
import dbms.geraltigas.utils.DataDump;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CreateIndexExec implements ExecPlan {

    String indexName;
    String tableName;
    String columnName;

    @Autowired
    LockManager lockManager;
    @Autowired
    DiskManager diskManager;
    @Autowired
    TableBuffer tableBuffer;

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
    public String execute(String dataPath) throws IOException, BlockException, DataDirException, DataTypeException {
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

        if (isTxn) {
            transactionExecutor.addChangeLog(new CreateFileChangeLog(indexFile.toString()));
        }

        long headerId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
        lockManager.lockWrite(headerId, threadId);
        IndexHeader indexHeader = new IndexHeader();
        diskManager.writeIndexHeader(tableName,indexName,indexHeader);

        // get all index data page write lock
        for (int i = 0; i < indexHeader.getIndexDataPageNum(); i++) {
            long indexDataPageId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX, indexName, i+1);
            lockManager.lockWrite(indexDataPageId, threadId);
        }

        long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE, null ,0);
        lockManager.lockRead(tableHeaderId, threadId);
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int tableDataPageNum = tableHeader.getTableLength();

        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

        // get index column index
        int indexColumnIndex = -1;
        for (int i = 0; i < tableDefine.getColNames().size(); i++) {
            if (tableDefine.getColNames().get(i).equals(columnName)) {
                indexColumnIndex = i;
                break;
            }
        }

        List<TableDefine.Type> indexTypes = new ArrayList<>();
        indexTypes.add(tableDefine.getColTypes().get(indexColumnIndex));
        indexTypes.add(TableDefine.Type.INTEGER); // page index
        indexTypes.add(TableDefine.Type.INTEGER); // record index
        List<List<String>> attrValues = new ArrayList<>();

        List<Object> indexData = new ArrayList<>(3);

        int indexDataLength = InsertExec.CalculateLength(indexTypes, attrValues);
        for (int i = 0; i < tableDataPageNum; i++) {
            long tableDataId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE, null ,i+1);
            lockManager.lockRead(tableDataId, threadId);
            PageHeader pageHeader = diskManager.readPageHeader(tableName, i+1);
            int recordNum = pageHeader.getRecordNum();
            int indexNum = 0;
            for (int j = 0; j < recordNum; j++) {
                byte[] record = diskManager.readOneRecord(tableName, i+1, j);
                if (record[0] == 0) {
                    continue;
                }

                List<Object> values = DataDump.load(tableDefine.getColTypes(), record,0);
                indexData.clear();
                indexData.add(values.get(indexColumnIndex));
                indexData.add(i+1);
                indexData.add(j);
                byte[] indexDataBytes = DataDump.dump(indexTypes, indexData);
                int hash = values.get(indexColumnIndex).toString().hashCode();
                hash = hash % indexHeader.getIndexHashArraySize();
                IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hash+1);
                diskManager.writeOneIndexData(tableName, hash+1, indexName, indexPageHeader.getIndexNum(), indexDataLength ,indexDataBytes);
                indexPageHeader.setIndexNum(indexPageHeader.getIndexNum()+1);
                diskManager.writeIndexPageHeader(tableName, indexName, hash+1, indexPageHeader);
                indexNum++;
            }
        }

        if(!isTxn) {
            lockManager.unlockAll(threadId);
        }

        return "Create index " + indexName + " on " + tableName+"."+columnName+ " success";
    }

    private byte[] getIndexDataFromRecord(byte[] record, int offset, int length) { // TODO: implement this
        byte[] indexData = new byte[length];
        System.arraycopy(record, offset, indexData, 0, length);
        return indexData;
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
