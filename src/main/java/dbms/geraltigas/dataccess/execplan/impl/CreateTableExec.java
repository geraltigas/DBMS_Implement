package dbms.geraltigas.dataccess.execplan.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.format.tables.TableHeader;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class CreateTableExec implements ExecPlan {
    private String tableName;
    private String[] colNames;
    private TableDefine.Type[] colTypes;
    private List<String>[] colAttrs;
    private long threadId;

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private TableBuffer tableBuffer;
    @Autowired
    private DiskManager diskManager; // use lock manager to manager this part to make it thread safe

    public CreateTableExec(String tableName, String[] colNames, String[] colTypes, List<String>[] colAttrs) throws DataTypeException {
        this.tableName = tableName;
        this.colNames = colNames;
        this.colTypes = new TableDefine.Type[colTypes.length];
        int k = 0;
        for (String i : colTypes) {
            switch (i.toUpperCase()) {
                case "INT":
                    this.colTypes[k] = TableDefine.Type.INTEGER;
                    break;
                case "VARCHAR":
                    this.colTypes[k] = TableDefine.Type.VARCHAR;
                    break;
                case "FLOAT":
                    this.colTypes[k] = TableDefine.Type.FLOAT;
                    break;
                default:
                    throw new DataTypeException("Unknown data type: " + i);

            }
            k++;
        }
        this.colAttrs = colAttrs;
    }
    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, DataDirException {
        List<String> res = new LinkedList<>();
        // create dirs and table files
        Path dataDir = Paths.get(dataPath);
        Path tableDir = dataDir.resolve("tables");
        if(!tableDir.toFile().exists()){
            Files.createDirectories(tableDir);
            res.add("Create table dir");
        }

        Path tableFile = tableDir.resolve(tableName + ".tbl");
        if (!tableFile.toFile().exists()) {
            Files.createFile(tableFile);
            res.add("Create table file");
        }else {
            res.add("Table file already exists");
            res.add("Create table failed");
            return String.join(";\n", res);
        }

        // write table meta
        Path tableDefineDir = dataDir.resolve("metadata");
        if (!tableDefineDir.toFile().exists()) {
            Files.createDirectories(tableDefineDir);
            res.add("Create table metadata dir");
        }
        Path tableDefineFile = tableDefineDir.resolve(tableName + ".meta");
        if (!tableDefineFile.toFile().exists()) {
            Files.createFile(tableDefineFile);
        }else {
            res.add("Table metadata file already exists");
            res.add("Create table failed");
            return String.join(";\n", res);
        }

        TableDefine tableDefine = new TableDefine(tableName, colNames, colTypes, colAttrs);

        tableBuffer.addTableDefine(tableName, tableDefine);

        File tableDefineFileObj = tableDefineFile.toFile();

        try {
            objectMapper.writeValue(tableDefineFileObj, tableDefine);
            res.add("Write table metadata");
        }catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        diskManager.writeTableFileHeader(tableName, new TableHeader());
        res.add("Write table header");
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
