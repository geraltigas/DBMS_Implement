package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.PageBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.indexs.IndexPageHeader;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.transaction.changelog.impl.*;
import dbms.geraltigas.utils.DataDump;
import dbms.geraltigas.utils.IndexUtils;
import dbms.geraltigas.utils.Pair;
import dbms.geraltigas.utils.Printer;
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
                    return "Field "+ definedColNames.get(i) +" not found in value list";
                } else {
                    try {
                        switch (tableDefine.getColTypes().get(i)) {
                            case INTEGER -> record.add(Integer.parseInt(value.get(map[i]).toString()));
                            case VARCHAR -> {
                                String valueS = value.get(map[i]).toString();
                                if (!valueS.contains("'")) {
                                    throw new NumberFormatException("Varchar value must be in ''");
                                }
                                record.add(valueS.replace("'", ""));
                            }
                            case FLOAT -> {
                                Float floatV = Float.parseFloat(value.get(map[i]).toString());
                                if (floatV.isInfinite() || floatV.isNaN()) {
                                    return "Value Format Exception, please insert valid values. \nFor input string: \"" + value.get(map[i]).toString() + "\"";
                                }
                                record.add(floatV);
                            }
                            default -> throw new FieldNotFoundException("Field " + definedColNames.get(i) + "not found");
                        }
                    } catch (NumberFormatException e) {
                        return "Value Format Exception, please insert valid values. \n" + e.getMessage();
                    }
                }
            }
            if (record.size() != definedColNames.size()) {
                return "Value Format Exception, please insert valid values. \n" + "Column not found";
            }
            records.add(record);
        }

        return insertRecords(records,tableDefine.getColTypes(),tableDefine.getColAttrs());
    }
    @Override
    public boolean getIsTxn() {
        return isTxn;
    }
    private String insertRecords(List<List<Object>> records, List<TableDefine.Type> colTypes, List<List<String>> colAttrs) throws BlockException, IOException, DataDirException, DataTypeException {
        if (records.size() != 1) {
            if (isTxn) lockManager.unlockAll(threadId);
            return "Not support multi insert";
        }
        long tableHeaderId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,0);
        lockManager.lockWrite(tableHeaderId,threadId);
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int per_size = CalculateLength(colTypes,colAttrs);
        int writeSize = per_size * records.size();
        byte[] data = new byte[writeSize];
        boolean isOk = DataDump.dumpSrc(data,per_size,colTypes,records);
        if (!isOk) {
            if (isTxn) lockManager.unlockAll(threadId);
            return "Value Format Exception, please insert valid values. \n"+"String length too long.";
        }
        if (tableHeader.getTableLength() == 0) {
            int pageNum = records.size()*per_size/(PageBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH);
            if (records.size()*per_size%(PageBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH) != 0) {
                pageNum++;
            }
            int recordPerBlock = (PageBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH)/per_size;
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
                    diskManager.setOneRecord(tableName,i+1,j,tempData);
                    insertWithIndex(records.get(0),i+1,j);
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
            PageHeader pageHeader = diskManager.getPageHeader(tableName,pageNum);
            int recordPerBlock = (PageBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH)/per_size;
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
                diskManager.setOneRecord(tableName,pageNum,oldRecordNum+i,tempData);
                insertWithIndex(records.get(0),pageNum,oldRecordNum+i);
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
                    System.arraycopy(data,0,dataTT,0,dataTT.length);
                    pageId = LockManager.computeId(tableName, DiskManager.AccessType.TABLE,null,pageNum+i+1);
                    lockManager.lockWrite(pageId,threadId);
                    if (isTxn) transactionExecutor.addChangeLog(new TablePageHeaderChangeLog(tableName,pageNum+i+1,oldPageHeaderT));
                    diskManager.setPageHeader(tableName,pageNum+i+1,pageHeaderT);
                    int recordNumT = dataTT.length/pageHeaderT.getRecordLength();
                    byte[] tempDataT = new byte[pageHeaderT.getRecordLength()];
                    for (int j = 0; j < recordNumT; j++) {
                        if (isTxn) transactionExecutor.addChangeLog(new RecordChangeLog(tableName,pageNum+i+1,j,new byte[per_size]));
                        System.arraycopy(dataTT,j * per_size,tempDataT,0,per_size);
                        diskManager.setOneRecord(tableName,pageNum+i+1,j,tempDataT);
                        insertWithIndex(records.get(0),pageNum+i+1,j);
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

    private void insertWithIndex(List<Object> record, int pageIdx, int recordIdx) throws IOException, DataTypeException, BlockException, DataDirException {
        Pair<List<String>,List<String>> pair = tableBuffer.getIndexNameAndIndexColumnNameList(tableName);
        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);
        List<String> indexNameList = pair.getFirst();
        List<String> indexColumnNameList = pair.getSecond();
        for (int i = 0; i < indexNameList.size(); i++) {
            List<String> columnList = tableDefine.getColNames();
            int columnIndex = columnList.indexOf(indexColumnNameList.get(i));
            List<TableDefine.Type> typeList = tableDefine.getColTypes();
            List<List<String>> attrList = tableDefine.getColAttrs();
            IndexUtils indexUtils = new IndexUtils(typeList.get(columnIndex),attrList.get(columnIndex));
            indexUtils.generateIndexDataBytes(record.get(columnIndex),pageIdx,recordIdx);
            int hash = record.get(columnIndex).toString().hashCode();
            String indexName = indexNameList.get(i);
            long indexHeaderPageId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,0);
            lockManager.lockRead(indexHeaderPageId,threadId);
            IndexHeader indexHeader = diskManager.getIndexHeader(tableName,indexName);
            hash %= indexHeader.getIndexHashArraySize();
            if (hash < 0) hash += indexHeader.getIndexHashArraySize();
            long indexPageId = LockManager.computeId(tableName, DiskManager.AccessType.INDEX,indexName,hash+1);
            lockManager.lockWrite(indexPageId,threadId);
            IndexPageHeader indexPageHeader = diskManager.getIndexPageHeader(tableName,indexName,hash+1);
            IndexPageHeader oldIndexPageHeader = new IndexPageHeader(indexPageHeader);
            int indexNum = indexPageHeader.getIndexNum();
            diskManager.setOneIndexData(tableName,hash+1,indexName,indexNum,indexUtils.getIndexDataLength(),indexUtils.generateIndexDataBytes());
            if (isTxn) transactionExecutor.addChangeLog(new IndexChangeLog(tableName,hash+1,indexName,indexNum,indexUtils.getIndexDataLength(),indexUtils.generateIndexDataBytes()));
            indexPageHeader.setIndexNum(indexPageHeader.getIndexNum()+1);
            diskManager.setIndexPageHeader(tableName,indexName,hash+1,indexPageHeader);
            if (isTxn) transactionExecutor.addChangeLog(new IndexPageHeaderChangeLog(tableName,indexName,hash+1,oldIndexPageHeader));
            Printer.print("insert index data in indexName: " + indexName + ", \nindex Page idx: " + (hash+1) + ", \ndata: " + record.get(columnIndex)+ ", \ndata page idx: " + pageIdx + ", \ndata record idx: " + recordIdx ,threadId);
        }
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

