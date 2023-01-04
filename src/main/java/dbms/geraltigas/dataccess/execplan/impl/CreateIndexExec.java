package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.Executor;
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
import dbms.geraltigas.utils.IndexUtils;
import dbms.geraltigas.utils.Printer;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
    public boolean getIsTxn() {
        return isTxn;
    }
    @Override
    public String execute(String dataPath) throws IOException, BlockException, DataDirException, DataTypeException {
        Path dataDir = Paths.get(dataPath);

        TableDefine tableDefine = null;

        try {
            tableDefine = tableBuffer.getTableDefine(tableName);
        }catch (IOException e) {
            return "Table " + tableName + " does not exist";
        }

        if (!tableDefine.getColNames().contains(columnName)) {
            return "Column " + indexName + " does not exist";
        }

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

        if (isTxn) transactionExecutor.addChangeLog(new CreateFileChangeLog(indexFile.toString()));

        long headerId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
        lockManager.lockWrite(headerId, threadId);
        IndexHeader indexHeader = new IndexHeader();
        diskManager.setIndexHeader(tableName,indexName,indexHeader);

        long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE, null ,0);
        lockManager.lockRead(tableHeaderId, threadId);
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int tableDataPageNum = tableHeader.getTableLength();

        // get index column index
        int indexColumnIndex = -1;
        for (int i = 0; i < tableDefine.getColNames().size(); i++) {
            if (tableDefine.getColNames().get(i).equals(columnName)) {
                indexColumnIndex = i;
                break;
            }
        }

        IndexUtils indexUtils = new IndexUtils(tableDefine.getColTypes().get(indexColumnIndex),tableDefine.getColAttrs().get(indexColumnIndex));

        int indexDataLength = indexUtils.getIndexDataLength();
        for (int i = 0; i < tableDataPageNum; i++) {
            long tableDataId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE, null ,i+1);
            lockManager.lockRead(tableDataId, threadId);
            PageHeader pageHeader = diskManager.getPageHeader(tableName, i+1);
            int recordNum = pageHeader.getRecordNum();
            int indexNum = 0;
            for (int j = 0; j < recordNum; j++) {
                byte[] record = diskManager.getOneRecord(tableName, i+1, j);
                if (record[0] == 0) {
                    continue;
                }

                List<Object> values = DataDump.load(tableDefine.getColTypes(), record,0);
                indexUtils.generateIndexDataObjectList(values.get(indexColumnIndex),i+1,j);
                byte[] indexDataBytes = indexUtils.generateIndexDataBytes();
                int hash = values.get(indexColumnIndex).toString().hashCode();
                hash = hash % indexHeader.getIndexHashArraySize();
                if (hash < 0) hash += indexHeader.getIndexHashArraySize();
                long indexDataPageId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX, indexName, hash+1);
                lockManager.lockWrite(indexDataPageId, threadId);
                IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName, indexName, hash+1);
                Printer.print("add index data in indexName : " + indexName + ", \nindexPageIndex: " + (hash+1) + ", \nindexDataIndex: "+indexPageHeader.getIndexNum()+ ", \ndata: "+ values.get(indexColumnIndex) + ", \ndataPageIdx: " + (i+1) +", \ndataPageRecordIdx: " + j,threadId);
                diskManager.setOneIndexData(tableName, hash+1, indexName, indexPageHeader.getIndexNum(), indexDataLength ,indexDataBytes);
                indexPageHeader.setIndexNum(indexPageHeader.getIndexNum()+1);
                diskManager.setIndexPageHeader(tableName, indexName, hash+1, indexPageHeader);
                indexNum++;
            }
        }

        if(!isTxn) {
            lockManager.unlockAll(threadId);
        }

        return "Create index " + indexName + " on " + tableName+"."+columnName+ " success";
    }

    private byte[] getIndexDataFromRecord(byte[] record, int offset, int length) {
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
