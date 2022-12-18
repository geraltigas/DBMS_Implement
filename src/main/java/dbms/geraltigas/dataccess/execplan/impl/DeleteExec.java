package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.expression.Expression;
import dbms.geraltigas.format.tables.Bulk;
import dbms.geraltigas.format.tables.TableDefine;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static dbms.geraltigas.buffer.BlockBuffer.BLOCK_SIZE;

public class DeleteExec implements ExecPlan { // TODO: implement this

    @Autowired
    TableBuffer tableBuffer;

    @Autowired
    DiskManager diskManager;

    String tableName;
    Expression whereExpression;

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        List<String> res = new ArrayList<>();

        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

        int recordNum = SelectExec.SELECT_PAGE_NUMBER*BLOCK_SIZE/tableDefine.getRecordLength();
        int index = 0;

        Bulk bulk = tableBuffer.getTableBulk(tableName);
        bulk.initBulk();

        int deleteNum = 0;
        int indexCount = 0;

        while (true) {
            byte[] records = diskManager.readRecords(tableName, index, recordNum, tableDefine.getRecordLength() ,diskManager.getTableHeader(tableName));
            index+=records.length/tableDefine.getRecordLength();
            if (records.length == 0) {
                bulk.flush();
                break;
            }
            byte[] record = new byte[tableDefine.getRecordLength()];

            for (int i = 0; i < records.length; i+=tableDefine.getRecordLength()) {
                indexCount++;
                System.arraycopy(records, i, record, 0, tableDefine.getRecordLength());
                // judge the record is valid
                if (record[0] == 1) {
                    // judge the record is valid
                    if (whereExpression != null) {
                        if (whereExpression.evalNoAlias(record, tableDefine)) {
                            record[0] = 0;
                            deleteNum++;
                            bulk.addBulk(indexCount);
                            diskManager.writeRecord(tableName, i/tableDefine.getRecordLength(), record, tableDefine.getRecordLength(), diskManager.getTableHeader(tableName));
                            indexCount++;
                        }
                    }else {
                        record[0] = 0;
                        deleteNum++;
                        bulk.addBulk(indexCount);
                        diskManager.writeRecord(tableName, i/tableDefine.getRecordLength(), record, tableDefine.getRecordLength(), diskManager.getTableHeader(tableName));
                        indexCount++;
                    }
                }
            }
        }

        res.add("Delete " + deleteNum + " records from table " + tableName);

        return String.join("\n", res);
    }

    public DeleteExec(String tableName, Expression whereExpression) {
        this.tableName = tableName;
        this.whereExpression = whereExpression;
    }
}
