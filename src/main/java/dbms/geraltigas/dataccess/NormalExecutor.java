package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.*;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
public class NormalExecutor implements Executor {

    private ConcurrentLinkedDeque<ExecPlan> execPlans = new ConcurrentLinkedDeque<>();

    private ExecuteEngine executeEngine;

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
                if (execPlans.size() > 0) {
                    ExecPlan execPlan = execPlans.poll();
                    assert execPlan != null;
                    String res = null;
                    try {
                        res = execPlan.execute(executeEngine.getDateDir());
                    } catch (IOException | DataTypeException | FieldNotFoundException | BlockException |
                             DataDirException e) {
                        throw new RuntimeException(e);
                    } catch (ThreadStopException e) {
                        throw new RuntimeException(e);
                    }
                    executeEngine.addResult(execPlan.hashCode(), res);
                }
        }
    }


}
