package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.dataccess.execplan.ExecPlan;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DropExec implements ExecPlan {
    String tableName;
    private long threadId;

    public DropExec(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String execute(String dataPath) {
        List<String> res = new ArrayList<>();
        Path path = Paths.get(dataPath + "/tables");
        if (path.toFile().exists()) {
            Path tablePath = path.resolve(tableName + ".tbl");
            if (tablePath.toFile().exists()) {
                tablePath.toFile().delete();
                res.add("Table " + tableName + " deleted");
            } else {
                res.add("Table " + tableName + " not exists");
                return String.join(";\n", res);
            }
        }
        Path indexDir = Paths.get(dataPath + "/indexes/" + tableName);
        if (indexDir.toFile().exists()) {
            for (File i : Objects.requireNonNull(indexDir.toFile().listFiles())) {
                i.delete();
                res.add("Index " + i.getName() + " deleted");
            }
            indexDir.toFile().delete();
            res.add("Index directory " + tableName + " deleted");
        }

        Path metaPath = Paths.get(dataPath + "/metadata/" + tableName + ".meta");
        if (metaPath.toFile().exists()) {
            metaPath.toFile().delete();
            res.add("Metadata " + tableName + " deleted");
        }
        res.add("Table " + tableName + " dropped");
        return String.join(";\n", res);
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
}
