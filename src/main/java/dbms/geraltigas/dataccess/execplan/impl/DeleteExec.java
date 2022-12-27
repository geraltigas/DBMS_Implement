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
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
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
    private long threadId;

    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        List<String> res = new ArrayList<>();

        TableDefine tableDefine = tableBuffer.getTableDefine(tableName);

        int recordNum = SelectExec.SELECT_PAGE_NUMBER*BLOCK_SIZE/tableDefine.getRecordLength();
        TableHeader tableHeader = new TableHeader(diskManager.readPage(tableName, 0));
        int tableLength = tableHeader.getTableLength();
        int deleteNum = 0;

        for (int index = 0; index < tableLength; index++) {
            byte[] records = diskManager.readRecords(tableName, index+1);
            byte[] pageData = diskManager.readPage(tableName, index+1);
            PageHeader pageHeader = new PageHeader(pageData);
            if (records.length == 0) {
                break;
            }
            byte[] record = new byte[tableDefine.getRecordLength()];
            int delTemp = 0;
            for (int i = 0; i < records.length/tableDefine.getRecordLength(); i+=1) {

                System.arraycopy(records, i*tableDefine.getRecordLength(), record, 0, tableDefine.getRecordLength());
                // judge the record is valid
                if (record[0] == 1) {
                    // judge the record is valid
                    if (whereExpression != null) {
                        if (whereExpression.evalNoAlias(record, tableDefine)) {
                            record[0] = 0;
                            deleteNum++;
                            delTemp++;
                            diskManager.writeRecord(tableName,index+1,i,record,tableDefine.getRecordLength(),pageHeader);
                        }
                    }else {
                        record[0] = 0;
                        deleteNum++;
                        delTemp++;
                        diskManager.writeRecord(tableName,index+1,i,record,tableDefine.getRecordLength(),pageHeader);
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

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
}
