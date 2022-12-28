package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.DataDirException;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;

@Component
public class ExecuteEngine {

    @Autowired
    ExecutorService executorService;

    @Autowired
    NormalExecutor normalExecutor;

    private ConcurrentHashMap<Integer, String> results = new ConcurrentHashMap<>();
    private Map<Long, Executor> transactions = new ConcurrentHashMap<>();

    private String dataPath = "E:/DBMSTEST";

    public void setDataDir(String path) {
        this.dataPath = path;
    }
    public String getDateDir() {
        return this.dataPath;
    }

    public void createDataDri() throws DataDirException {
        File temp = new File(this.dataPath);
        if (temp.exists()) {
            Boolean res = temp.delete();
            if (!res) {
                throw new DataDirException("Data dir already exists, but failed to delete it");
            }
            res = temp.mkdir();
            if (!res) {
                throw new DataDirException("Failed to create data dir");
            }
        }
    }

    public void addExecPlan(ExecPlan execPlan) {
        if (transactions.containsKey(execPlan.getThreadId())) {
            // add execplan to transaction to shedule
            transactions.get(execPlan.getThreadId()).addExecplan(execPlan);
        } else {
            normalExecutor.addExecplan(execPlan);
        }
    }

    public String getResult(int hash) {
        return this.results.get(hash);
    }
    public void addResult(int hash, String result) {
        this.results.put(hash, result);
    }

    public void beginTxn(long threadId) {
        Executor executor = new TransactionExecutor(threadId);
        executor.setExecuteEngine(this);
        transactions.put(threadId,executor);
        executorService.submit(executor);
    }

    @PostConstruct
    private void beginDataAccessWatcher() {
        normalExecutor.setExecuteEngine(this);
        executorService.submit(normalExecutor);
    }
}