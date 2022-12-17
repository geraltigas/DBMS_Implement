package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.utils.DataDump;
import net.sf.jsqlparser.expression.Expression;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
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

    private String insertRecords(List<List<Object>> records, List<TableDefine.Type> colTypes, List<List<String>> colAttrs) throws BlockException, IOException, DataDirException, DataTypeException {
        TableHeader tableHeader = diskManager.getTableHeader(tableName);
        int offset = tableHeader.getEndOffset();
        int per_size = CalculateLength(colTypes,colAttrs);
        int writeSize = per_size * records.size();
        byte[] data = new byte[writeSize];
        DataDump.DumpSrc(data,per_size,colTypes,records);
        diskManager.writeBytesAt(tableName, DiskManager.AccessType.TABLE,null,data, offset);
        tableHeader.setEndOffset(tableHeader.getEndOffset()+ writeSize);
        tableHeader.setTableLength((offset + writeSize) / BlockBuffer.BLOCK_SIZE + 1);
        diskManager.setTableHeader(tableName,tableHeader);
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

