package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.transaction.changelog.ChangeLog;

public abstract class Executor extends Thread{
    void addExecplan(ExecPlan execPlan){};
    void setExecuteEngine(ExecuteEngine executeEngine){};
    public void addChangeLog(ChangeLog changeLog){};

}
