package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.utils.DataDump;
import net.sf.jsqlparser.expression.Expression;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class InsertExec implements ExecPlan {
    String tableName;
    String[] colNames;
    List<List<Expression>> values;

    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;
    private long threadId;


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
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int per_size = CalculateLength(colTypes,colAttrs);
        int writeSize = per_size * records.size();
        byte[] data = new byte[writeSize];
        DataDump.DumpSrc(data,per_size,colTypes,records);
        if (tableHeader.getTableLength() == 0) {
            int pageNum = records.size()*per_size/(BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH);
            if (records.size()*per_size%(BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH) != 0) {
                pageNum++;
            }
            int recordPerBlock = (BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH)/per_size;
            List<PageHeader> tableHeaders = new ArrayList<>(pageNum);
            for (int i = 0; i < pageNum; i++) {
                tableHeaders.add(new PageHeader());
            }
            for (int i = 0; i < pageNum; i++) {
                PageHeader pageHeader = tableHeaders.get(i);
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
                System.arraycopy(data,per_size*recordPerBlock*i,dataT,0,dataT.length);
                diskManager.writePage(tableName,i+1,0,tableHeaders.get(i).ToBytes());
                diskManager.writePage(tableName,i+1,pageHeader.getLastRecordOffset(),dataT);
            }
            tableHeader.setTableLength(pageNum);
            diskManager.setTableHeader(tableName,tableHeader);
        }else {
            int pageNum = tableHeader.getTableLength();
            byte[] pageHeaderBytes = diskManager.readBytesAt(tableName, DiskManager.AccessType.TABLE, null, (long) (pageNum)*BlockBuffer.BLOCK_SIZE, PageHeader.PAGE_HEADER_LENGTH);
            PageHeader pageHeader = new PageHeader(pageHeaderBytes);
            int recordPerBlock = (BlockBuffer.BLOCK_SIZE - PageHeader.PAGE_HEADER_LENGTH)/per_size;
            int firstPageRecordNum = pageHeader.getRecordNum();
            byte[] dataT = new byte[(records.size() > recordPerBlock - firstPageRecordNum ? recordPerBlock - firstPageRecordNum : records.size())*per_size];
            System.arraycopy(data,0,dataT,0,dataT.length);
            pageHeader.setRecordNum(records.size() > recordPerBlock - firstPageRecordNum ?recordPerBlock : records.size()+firstPageRecordNum);
            pageHeader.setLastRecordOffset(pageHeader.getLastRecordOffset() - dataT.length);
            diskManager.writePage(tableName,pageNum,pageHeader.getLastRecordOffset(),dataT);
            diskManager.writePage(tableName,pageNum,0,pageHeader.ToBytes());
            if (records.size() > firstPageRecordNum) {
                int recordNum = records.size() - firstPageRecordNum;
                int pageNumT = recordNum/recordPerBlock;
                if (recordNum%recordPerBlock != 0) {
                    pageNumT++;
                }
                for (int i = 0; i < pageNumT; i++) {
                    PageHeader pageHeaderT = new PageHeader();
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
                    diskManager.writePage(tableName,pageNum+i,0,pageHeaderT.ToBytes());
                    diskManager.writePage(tableName,pageNum+i,pageHeaderT.getLastRecordOffset(),dataTT);
                }
                tableHeader.setTableLength(pageNum+pageNumT);
            }
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

