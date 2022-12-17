package dbms.geraltigas.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dbms.geraltigas.dataccess.ExecList;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.tables.Bulk;
import dbms.geraltigas.format.tables.TableDefine;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;


@Component
public class TableBuffer {
    private Map<String, TableDefine> tableDefineMap = new HashMap<>();
    private Map<String, Bulk> tableDataMap = new HashMap<>();
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    ExecList execList;

    public Bulk getTableBulk(String tableName) throws BlockException, DataDirException, IOException {
        if (tableDataMap.containsKey(tableName)) {
            return tableDataMap.get(tableName);
        } else {
            return new Bulk(tableName);
        }
    }

    public void addTableDefine(String tableName, TableDefine tableDefine) {
        tableDefineMap.put(tableName, tableDefine);
    }

    public TableDefine getTableDefine(String tableName) {
        if (tableDefineMap.containsKey(tableName)) {
            return tableDefineMap.get(tableName);
        }else {
            TableDefine tableDefine = getTableDefineFromFile(execList.getDateDir(), tableName);
            tableDefineMap.put(tableName, tableDefine);
            return tableDefine;
        }
    }

    private TableDefine getTableDefineFromFile(String dataPath, String tableName) {
        Path path = Path.of(dataPath).resolve("metadata").resolve(tableName + ".meta");
        File metadataFile = path.toFile();
        try {
            TableDefine tableDefine = objectMapper.readValue(metadataFile, TableDefine.class);
            return tableDefine;
        } catch (Exception e) {
             e.printStackTrace();
            return null;
        }
    }
}
