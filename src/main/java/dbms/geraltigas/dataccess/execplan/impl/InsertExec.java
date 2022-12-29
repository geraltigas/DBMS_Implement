package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.transaction.changelog.impl.RecordChangeLog;
import dbms.geraltigas.transaction.changelog.impl.TableHeaderChangeLog;
import dbms.geraltigas.transaction.changelog.impl.TablePageHeaderChangeLog;
import dbms.geraltigas.utils.DataDump;
import net.sf.jsqlparser.expression.Expression;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;

public class InsertExec implements ExecPlan {
    String tableName;
    String[] colNames;
    List<List<Expression>> values;

    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;
    @Autowired
    LockManager lockManager;
    private long threadId;
    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }

    public InsertExec(String tableName, String[] colNames, List<List<Expression>> values) {
        this.tableName = tableName;
        this.colNames = colNames;
        this.values = values;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColNames() {
        return colNames;
    }

    public List<List<Expression>> getValues() {
        return values;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
    @Override
    public long getThreadId() {
        return this.threadId;
    }

    @Override
    public String execute(String dataPath) throws FieldNotFoundException, BlockException, IOException, DataDirException, DataTypeException {
        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

        List<List<Object>> records = new LinkedList<>();

        List<String> definedColNames = tableDefine.getColNames();

        byte[] map = GetOrderArray(definedColNames, colNames);

        for (List<Expression> value : values) {
            List<Object> record = new LinkedList<>();
            for (int i = 0; i < map.length; i++) {
                if (map[i] == -1) {
                    throw new FieldNotFoundException("Field "+ definedColNames.get(i) +"not found");
                } else {
                    switch (tableDefine.getColTypes().get(i)) {
                        case INTEGER -> record.add(Integer.parseInt(value.get(map[i]).toString()));
                        case VARCHAR -> record.add(value.get(map[i]).toString());
                        default -> throw new FieldNotFoundException("Field " + definedColNames.get(i) + "not found");
                    }
                }
            }
            records.add(record);
        }

        return insertRecords(records,tableDefine.getColTypes(),tableDefine.getColAttrs());
    }

    private String insertRecords(List<List<Object>> records, List<TableDefine.Type> colTypes, List<List<String>> colAttrs) throws BlockException, IOException, DataDirException, DataTypeException { // TODO: need massive test
        long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,0);
        lockManager.lockWrite(tableHeaderId,threadId);
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int per_size = CalculateLength(colTypes,colAttrs);
        int writeSize = per_size * records.size();
        byte[] data = new byte[writeSize];
        DataDump.dumpSrc(data,per_size,colTypes,records);
        if (tableHeader.getTableLength() == 0) {
            int pageNum = records.size()*per_size/(BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH);
            if (records.size()*per_size%(BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH) != 0) {
                pageNum++;
            }
            int recordPerBlock = (BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH)/per_size;
            List<PageHeader> pageHeaders = new ArrayList<>(pageNum);
            for (int i = 0; i < pageNum; i++) {
                pageHeaders.add(new PageHeader());
            }
            for (int i = 0; i < pageNum; i++) {
                PageHeader pageHeader = pageHeaders.get(i);
                PageHeader oldPageHeader = new PageHeader(pageHeader);
                long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,i+1);
                lockManager.lockWrite(pageId,threadId);
                if (i == pageNum - 1) {
                    pageHeader.setRecordLength(per_size);
                    pageHeader.setRecordNum(records.size()- recordPerBlock*i);
                    pageHeader.setLastRecordOffset(4096 - per_size*(records.size()- recordPerBlock*i));
                } else {
                    pageHeader.setRecordLength(per_size);
                    pageHeader.setRecordNum(recordPerBlock);
                    pageHeader.setLastRecordOffset(4096 - per_size*recordPerBlock);
                }
                byte[] dataT = new byte[pageHeader.getRecordNum()*pageHeader.getRecordLength()];
                int formerRecordNum = (pageNum-1)*recordPerBlock;
                System.arraycopy(data,formerRecordNum*per_size,dataT,0,dataT.length);
                int recordNum = dataT.length/pageHeader.getRecordLength();
                byte[] tempData = new byte[pageHeader.getRecordLength()];
                if (isTxn) transactionExecutor.addChangeLog(new TablePageHeaderChangeLog(tableName,i+1,oldPageHeader));
                diskManager.setPageHeader(tableName,i+1,new PageHeader(per_size));
                for (int j = 0; j < recordNum; j++) {
                    System.arraycopy(dataT,j * per_size,tempData,0,per_size);
                    if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName,i+1,j,new byte[per_size]));
                    diskManager.writeOneRecord(tableName,i+1,j,tempData);
                }
                diskManager.setPageHeader(tableName,i+1,pageHeader);
            }
            TableHeader oldTableHeader = new TableHeader(tableHeader);
            tableHeader.setTableLength(pageNum);
            if (isTxn) transactionExecutor.addChangeLog(new TableHeaderChangeLog(tableName,oldTableHeader));
            diskManager.setTableHeader(tableName,tableHeader);
        }else {
            int pageNum = tableHeader.getTableLength();
            long pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageNum);
            lockManager.lockWrite(pageId,threadId);
            byte[] pageHeaderBytes = diskManager.readBytesAt(tableName, DiskManager.AccessType.TABLE, null, (long) (pageNum)*BlockBuffer.BLOCK_SIZE, PageHeader.PAGE_HEADER_LENGTH);
            PageHeader pageHeader = new PageHeader(pageHeaderBytes);
            int recordPerBlock = (BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH)/per_size;
            int firstPageRecordNum = pageHeader.getRecordNum();
            byte[] dataT = new byte[(records.size() > recordPerBlock - firstPageRecordNum ? recordPerBlock - firstPageRecordNum : records.size())*per_size];
            System.arraycopy(data,0,dataT,0,dataT.length);
            PageHeader oldPageHeader = new PageHeader(pageHeader);
            int oldRecordNum = pageHeader.getRecordNum();
            pageHeader.setRecordNum(records.size() > recordPerBlock - firstPageRecordNum ?recordPerBlock : records.size()+firstPageRecordNum);
            pageHeader.setLastRecordOffset(pageHeader.getLastRecordOffset() - dataT.length);

            if (isTxn) transactionExecutor.addChangeLog(new TablePageHeaderChangeLog(tableName,pageNum,oldPageHeader));
            diskManager.setPageHeader(tableName,pageNum,pageHeader);
            int recordNum = dataT.length/pageHeader.getRecordLength();
            byte[] tempData = new byte[pageHeader.getRecordLength()];
            for (int i = 0; i < recordNum; i++) {
                if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName,pageNum,oldRecordNum+i,new byte[per_size]));
                System.arraycopy(dataT,i * per_size,tempData,0,per_size);
                diskManager.writeOneRecord(tableName,pageNum,oldRecordNum+i,tempData);
            }
            if (records.size() > recordNum) {
                recordNum = records.size() - recordNum;
                int pageNumT = recordNum/recordPerBlock;
                if (recordNum%recordPerBlock != 0) {
                    pageNumT++;
                }
                for (int i = 0; i < pageNumT; i++) {
                    PageHeader pageHeaderT = new PageHeader();
                    PageHeader oldPageHeaderT = new PageHeader(pageHeaderT);
                    if (i == pageNumT - 1) {
                        pageHeaderT.setRecordLength(per_size);
                        pageHeaderT.setRecordNum(recordNum - recordPerBlock*i);
                        pageHeaderT.setLastRecordOffset(4096 - per_size*(recordNum - recordPerBlock*i));
                    } else {
                        pageHeaderT.setRecordLength(per_size);
                        pageHeaderT.setRecordNum(recordPerBlock);
                        pageHeaderT.setLastRecordOffset(4096 - per_size*recordPerBlock);
                    }
                    byte[] dataTT = new byte[pageHeaderT.getRecordNum()*pageHeaderT.getRecordLength()];
                    System.arraycopy(data,per_size*recordPerBlock*i+firstPageRecordNum*per_size,dataTT,0,dataTT.length);
                    pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageNum+i+1);
                    lockManager.lockWrite(pageId,threadId);
                    if (isTxn) transactionExecutor.addChangeLog(new TablePageHeaderChangeLog(tableName,pageNum+i+1,oldPageHeaderT));
                    diskManager.setPageHeader(tableName,pageNum+i+1,pageHeaderT);
                    int recordNumT = dataTT.length/pageHeaderT.getRecordLength();
                    byte[] tempDataT = new byte[pageHeaderT.getRecordLength()];
                    for (int j = 0; j < recordNumT; j++) {
                        if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName,pageNum+i+1,j,new byte[per_size]));
                        System.arraycopy(dataTT,j * per_size,tempDataT,0,per_size);
                        diskManager.writeOneRecord(tableName,pageNum+i+1,j,tempDataT);
                    }
                }
                TableHeader oldTableHeader = new TableHeader(tableHeader);
                tableHeader.setTableLength(pageNum+pageNumT);
                if (isTxn) transactionExecutor.addChangeLog(new TableHeaderChangeLog(tableName,oldTableHeader));
                diskManager.setTableHeader(tableName,tableHeader);
            }
        }

        if(!isTxn) {
            lockManager.unlockAll(threadId);
        }

        return "Table " + tableName + " insert " + records.size() + " records";
    }

    public static byte[] GetOrderArray(List<String> definedColNames, String[] colNames) {
        byte[] map = new byte[definedColNames.size()];
        Arrays.fill(map, (byte)-1);
        for (int i = 0; i < definedColNames.size(); i++) {
            String colName = definedColNames.get(i);
            for (int j = 0; j < colNames.length; j++) {
                if (colName.equals(colNames[j])) {
                    map[i] = (byte)j;
                }
            }
        }
        return map;
    }

    public static int CalculateLength(List<TableDefine.Type> types, List<List<String>> attrs) throws DataTypeException {
        int length = 0;
        for (int i = 0; i < types.size(); i++) {
            switch (types.get(i)) {
                case INTEGER, FLOAT -> length += 4;
                case VARCHAR -> {
                    if (attrs.get(i) == null) {
                        throw new DataTypeException("Varchar type must have length");
                    } else {
                        length += Integer.parseInt(attrs.get(i).get(0)) + 4;
                    }
                }
            }
        }
        return length+1; // add the valid byte
    }
}

