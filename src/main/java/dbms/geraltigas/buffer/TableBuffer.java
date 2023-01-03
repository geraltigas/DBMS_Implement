package dbms.geraltigas.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.tables.Bulk;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.utils.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Component
public class TableBuffer { // TableBuffer is thread safe
    private Map<String, TableDefine> tableDefineMap = new ConcurrentHashMap<>();
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    ExecuteEngine executeEngine;


    public void addTableDefine(String tableName, TableDefine tableDefine) {
        tableDefineMap.put(tableName, tableDefine);
    }

    public synchronized TableDefine getTableDefine(String tableName) throws IOException {
        if (tableDefineMap.containsKey(tableName)) {
            return tableDefineMap.get(tableName);
        }else {
            try{
                TableDefine tableDefine = getTableDefineFromFile(executeEngine.getDateDir(), tableName);
                tableDefineMap.put(tableName, tableDefine);
                return tableDefine;
            }catch (IOException e) {
                throw new IOException("Table " + tableName + " not found");
            }

        }
    }

    private TableDefine getTableDefineFromFile(String dataPath, String tableName) throws IOException {
        Path path = Path.of(dataPath).resolve("metadata").resolve(tableName + ".meta");
        File metadataFile = path.toFile();
        TableDefine tableDefine = objectMapper.readValue(metadataFile, TableDefine.class);
        return tableDefine;
    }

    public Pair<List<String>, List<String>> getIndexNameAndIndexColumnNameList(String tableName) {
        List<String> indexNameList = new ArrayList<>();
        List<String> indexColumnNameList = new ArrayList<>();
        Path path = Path.of(executeEngine.getDateDir()).resolve("indexes").resolve(tableName);
        String[] indexFiles = path.toFile().list();
        if (indexFiles == null) {
            return new Pair<>(indexNameList, indexColumnNameList);
        }
        for (String fileName : indexFiles) {
            String[] split = fileName.split("[\\[\\]]");
            indexNameList.add(split[0]);
            indexColumnNameList.add(split[1].split("[\\(\\)]")[1]);
        }
        return new Pair<>(indexNameList, indexColumnNameList);
    }
}
