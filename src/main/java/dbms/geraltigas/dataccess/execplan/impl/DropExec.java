package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.TransactionExecutor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.format.tables.TableHeader;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DropExec implements ExecPlan {
    String tableName;
    private long threadId;
    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }
    public DropExec(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String execute(String dataPath) {
        List<String> res = new ArrayList<>();
        if (isTxn) {
            return "Dont support drop in txn";
        }
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

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
    @Override
    public long getThreadId() {
        return this.threadId;
    }
}
