package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.dataccess.Executor;
import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class ShowExec implements ExecPlan {
    private long threadId;
    private String variable;

    public ShowExec(String variable) {
        this.variable = variable.split(" ")[1];
    }
    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
    @Override
    public long getThreadId() {
        return this.threadId;
    }

    boolean isTxn;
    Executor transactionExecutor;
    public void setTxn(boolean txn, Executor executor) {
        isTxn = txn;
        this.transactionExecutor =  executor;
    }
    @Override
    public String execute(String dataPath) throws IOException, DataTypeException, FieldNotFoundException, BlockException, DataDirException {
        switch (variable) {
            case "tables" -> {
                Path path = Path.of(dataPath).resolve("tables");
                return Arrays.toString(Arrays.stream(path.toFile().list()).map(s -> s.replace(".tbl", "")).toArray());
            }
            case "drop" -> {
                return "Dont support drop in txn";
            }
            default -> {
                return "unknown";
            }
        }
    }
}
