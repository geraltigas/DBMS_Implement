package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.utils.Pair;
import dbms.geraltigas.utils.Printer;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static dbms.geraltigas.utils.Printer.DEBUG;

@Component
public class ExecuteEngine {


    @Autowired
    NormalExecutor normalExecutor;

    private ConcurrentHashMap<Integer, String> results = new ConcurrentHashMap<>();
    public Map<Long, TransactionExecutor> transactions = new ConcurrentHashMap<>();

    private Map<Long, Integer> txnStep = new ConcurrentHashMap<>();

    private String dataPath = "E:/DBMSTEST";

    @Autowired
    ExecutorService executorService;

    @Autowired
    LockManager lockManager;

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

    public boolean hasTxn() {
        return !transactions.isEmpty();
    }

    public boolean existTxn(long threadId) {
//        if (threadId == -1) return true;
        return transactions.containsKey(threadId);
    }

    public boolean existExecutor(long threadId) {
        return existTxn(threadId) || threadId == normalExecutor.nowThreadId;
    }

    public void beginTxn(long threadId) {
        TransactionExecutor executor = new TransactionExecutor(threadId);
        executor.setExecuteEngine(this);
        transactions.put(threadId,executor);
        executorService.submit(executor);
    }

    public void commitTxn(long threadId) {
        transactions.remove(threadId);
    }

    public synchronized void rollbackTxn(long threadId) throws BlockException, DataDirException, IOException {
        TransactionExecutor executor = (TransactionExecutor) transactions.get(threadId);
        if (executor == null) {
            return;
        }
        executor.stop();

        int hash = executor.rollBack();
        transactions.remove(threadId);
        lockManager.unlockAll(threadId);
        results.put(hash, "Transaction rollbacked");
    }

    public synchronized void rollbackTxns(List<TransactionExecutor> transactionExecutors) throws BlockException, DataDirException, IOException {
        List<Integer> hashs = new ArrayList<>();
        for (TransactionExecutor executor : transactionExecutors) {
            hashs.add(executor.rollBack());
        }
        for (TransactionExecutor executor : transactionExecutors) {
            transactions.remove(executor.threadId);
        }
        for (int i = 0; i < transactionExecutors.size(); i++) {
            transactionExecutors.get(i).stop();
            lockManager.unlockAll(transactionExecutors.get(i).threadId);
            results.put(hashs.get(i), "Transaction rollbacked");
        }
    }



    public void addExecPlan(ExecPlan execPlan) {
        if (transactions.containsKey(execPlan.getThreadId()) && !transactions.get(execPlan.getThreadId()).isLastRollBackOrCommit()) {
            // add execplan to transaction to shedule
            Printer.print("Add execplan to transaction" + execPlan.getThreadId(), "info");
            execPlan.setTxn(true, transactions.get(execPlan.getThreadId()));
            transactions.get(execPlan.getThreadId()).addExecplan(execPlan);
        } else {
            Printer.print("Add to normal executor", "info");
            execPlan.setTxn(false,normalExecutor);
            normalExecutor.addExecplan(execPlan);
        }
    }

    public String getResult(int hash) {
        return this.results.get(hash);
    }
    public void addResult(int hash, String result) {
        this.results.put(hash, result);
    }


    @PostConstruct
    private void beginDataAccessWatcher() {
        normalExecutor.setExecuteEngine(this);
        executorService.submit(normalExecutor);
        lockManager.setExecuteEngine(this);
        System.out.println("[ExecuteEngine] NormalExecutor started");
    }

    public void detectDeadLock() throws BlockException, DataDirException, IOException {
        List<Long> txnStepToRemove = new ArrayList<>();
        List<Long> txnToRollback = new ArrayList<>();
        DEBUG("Former txn step: " + txnStep);

        if (txnStep.isEmpty()) {
            for (Map.Entry<Long, TransactionExecutor> entry : transactions.entrySet()) {
                txnStep.put(entry.getKey(), entry.getValue().getStep());
            }
        } else {
            for (Map.Entry<Long, TransactionExecutor> entry : transactions.entrySet()) {
                if (txnStep.containsKey(entry.getKey())) {
                    int step = txnStep.get(entry.getKey());
                    if (step == entry.getValue().getStep()) {
                        txnToRollback.add(entry.getKey());
                    } else {
                        txnStep.put(entry.getKey(), entry.getValue().getStep());
                    }
                } else {
                    txnStep.put(entry.getKey(), entry.getValue().getStep());
                }
            }
            for (Map.Entry<Long,Integer> entry : txnStep.entrySet()) {
                if (!transactions.containsKey(entry.getKey())) {
                    txnStepToRemove.add(entry.getKey());
                }
            }
        }

        DEBUG("DeadLock detected, rollback txn: " + txnToRollback);

        for (Long key : txnStepToRemove) {
            txnStep.remove(key);
        }

        List<TransactionExecutor> transactionExecutors = new ArrayList<>();

        for (Long key : txnToRollback) {
            transactionExecutors.add(transactions.get(key));
        }

        rollbackTxns(transactionExecutors);

        DEBUG("Current txn step: " + txnStep);

    }

    private AtomicBoolean normalStop = new AtomicBoolean(false);

    public void setNormalStop(boolean stop) {
        this.normalStop.set(stop);
    }

    public boolean getNormalStop() {
        return this.normalStop.get();
    }

    public void endTransaction(long threadId) {
        Executor executor = transactions.get(threadId);
        if (executor != null) {
            executor.stop();
        }
        // remove thread in executeService
    }
}
