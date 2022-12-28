package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.transaction.changelog.ChangeLog;

public interface Executor extends Runnable{
    void addExecplan(ExecPlan execPlan);
    void setExecuteEngine(ExecuteEngine executeEngine);

    void addChangeLog(ChangeLog changeLog);
}
