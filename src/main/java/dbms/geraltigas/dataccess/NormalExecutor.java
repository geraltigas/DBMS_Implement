package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    @SneakyThrows
    @Override
    public void run() {
        System.out.println("[ExecuteEngine] NormalExecutor begin");
        while (true) {
                if (execPlans.size() > 0) {
                    ExecPlan execPlan = execPlans.poll();
                    assert execPlan != null;
                    String res = execPlan.execute(executeEngine.getDateDir());
                    executeEngine.addResult(execPlan.hashCode(), res);
                }
        }
    }
}
