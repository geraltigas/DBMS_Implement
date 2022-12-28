package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;

public interface Executor extends Runnable{
    void addExecplan(ExecPlan execPlan);
    void setExecuteEngine(ExecuteEngine executeEngine);
}
