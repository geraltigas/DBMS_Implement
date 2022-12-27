package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateIndexExec implements ExecPlan { // TODO: implement this
    String indexName;
    String tableName;
    String columnName;

    private long threadId;

    public CreateIndexExec(String indexName, String tableName, String columnName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public String execute(String dataPath) throws IOException {
        Path dataDir = Paths.get(dataPath);
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

        Path indexFile = indexDir.resolve(indexName+".idx");
        if (indexFile.toFile().exists()) {
            return "Index already exists";
        }

        boolean succ = indexFile.toFile().createNewFile();
        if (!succ) {
            return "Create index " + indexName + " on " + tableName+"."+columnName+ " failed";
        }

        // TODO: here should generic index data and write it to index file


        return "Create index " + indexName + " on " + tableName+"."+columnName+ " success";
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
}
