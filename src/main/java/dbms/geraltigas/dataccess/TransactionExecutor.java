package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.FieldNotFoundException;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import lombok.SneakyThrows;

import javax.swing.event.ChangeEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class TransactionExecutor implements Executor{
    long threadId;
    private ConcurrentLinkedDeque<ExecPlan> execPlans = new ConcurrentLinkedDeque<>();

    private ExecuteEngine executeEngine;

    private List<ChangeLog> changeLogs = new ArrayList<>();



    public TransactionExecutor(long threadId) {
        this.threadId = threadId;
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
                    res = execPlan.execute(executeEngine.getDateDir());
                } catch (IOException | DataTypeException | FieldNotFoundException | BlockException | DataDirException e) {
                    throw new RuntimeException(e);
                }
                executeEngine.addResult(execPlan.hashCode(), res);
            }
        }
    }
}
