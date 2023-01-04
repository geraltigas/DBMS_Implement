package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.*;
import dbms.geraltigas.transaction.LockManager;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
public class NormalExecutor extends Executor {

    private ConcurrentLinkedDeque<ExecPlan> execPlans = new ConcurrentLinkedDeque<>();

    private ExecuteEngine executeEngine;

    @Autowired
    LockManager lockManager;

    @Override
    public void addExecplan(ExecPlan execPlan) {
        execPlans.add(execPlan);
    }

    @Override
    public void setExecuteEngine(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public void addChangeLog(ChangeLog changeLog) {
    }

    @Override
    public void run() {
        System.out.println("[ExecuteEngine] NormalExecutor begin");
        while (true) {
            if (execPlans.size() > 0 && !executeEngine.getNormalStop()) {
                ExecPlan execPlan = execPlans.poll();
                assert execPlan != null;
                String res = null;
                try {
                    res = execPlan.execute(executeEngine.getDateDir());
                    if (!execPlan.getIsTxn()) lockManager.unlockAll(execPlan.getThreadId());
                } catch (IOException | DataTypeException | FieldNotFoundException | BlockException |
                         DataDirException e) {
                    exceptionHandler(e,res);
                } catch (ThreadStopException e) {
                    applicationStopHandler(e,res);
                }
                executeEngine.addResult(execPlan.hashCode(), res);
            }
        }
    }

    private void applicationStopHandler(ThreadStopException e,String res) {
    }

    private void exceptionHandler(Exception e,String res) {
        throw new RuntimeException(e);
    }


}
