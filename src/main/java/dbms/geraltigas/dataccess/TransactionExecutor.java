package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.dataccess.execplan.impl.CommitTxnExec;
import dbms.geraltigas.dataccess.execplan.impl.RollbackTxnExec;
import dbms.geraltigas.exception.*;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.swing.event.ChangeEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import static dbms.geraltigas.utils.Printer.DEBUG;

public class TransactionExecutor extends Executor{
    long threadId;
    private ConcurrentLinkedDeque<ExecPlan> execPlans = new ConcurrentLinkedDeque<>();

    @Getter
    private int step = 0;

    private ExecuteEngine executeEngine;

    private int nowHash = 0;

    private List<ChangeLog> changeLogs = new ArrayList<>();

    public TransactionExecutor(long threadId) {
        this.threadId = threadId;
    }

    public int rollBack() throws BlockException, DataDirException, IOException {
        // reverse for each change log
        for (int i = changeLogs.size() - 1; i >= 0; i--) {
            DEBUG("Rolling back change log " + i);
            changeLogs.get(i).recover();
        }
        executeEngine.endTransaction(threadId);
        return nowHash;
//        for (ChangeLog changeLog : changeLogs) {
//            changeLog.recover();
//        }
    }


    @Override
    public void addExecplan(ExecPlan execPlan) {
        execPlans.add(execPlan);
    }

    @Override
    public void setExecuteEngine(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public synchronized void addChangeLog(ChangeLog changeLog) {
        changeLogs.add(changeLog);
    }

    @Override
    public void run() {
        System.out.println("[ExecuteEngine] TransactionExecutor " + threadId + " begin");
        while (true) {
            if (execPlans.size() > 0) {
                ExecPlan execPlan = execPlans.poll();
                assert execPlan != null;
                String res = null;
                try {
                    nowHash = execPlan.hashCode();
                    res = execPlan.execute(executeEngine.getDateDir());
                    step++;
                } catch (IOException | DataTypeException | FieldNotFoundException | BlockException | DataDirException e) {
                    exceptionHandler(e,res);
                } catch (ThreadStopException e) {
                    System.out.println("[ExecuteEngine] TransactionExecutor " + threadId + " stop");
                    executeEngine.addResult(execPlan.hashCode(), "Transaction stop");
                    return;
                } catch (InterruptedException e) {
                    System.out.println("[ExecuteEngine] TransactionExecutor " + threadId + " stop");
                    return;
                }
                executeEngine.addResult(execPlan.hashCode(), res);
            }
        }
    }

    private void exceptionHandler(Exception e, String res) {
        throw new RuntimeException(e);
    }

    public boolean isLastRollBackOrCommit() {
        if (execPlans.size() == 0) return false;
        return execPlans.getLast() instanceof RollbackTxnExec || execPlans.getLast() instanceof CommitTxnExec;
    }
}
