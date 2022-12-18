package dbms.geraltigas.dataccess;

import dbms.geraltigas.dataccess.execplan.ExecPlan;
import dbms.geraltigas.exception.DataDirException;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;

@Component
public class ExecList {

    @Autowired
    ExecutorService executorService;

    private ConcurrentLinkedDeque<ExecPlan> execPlans = new ConcurrentLinkedDeque<>();
    private ConcurrentHashMap<Integer, String> results = new ConcurrentHashMap<>();

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
        this.execPlans.add(execPlan);
    }

    public String getResault(int hash) {
        return this.results.get(hash);
    }

    @PostConstruct
    private void beginDataAccessWatcher() {
        System.out.println("DataAccesser begin to watch");
        executorService.submit(() -> {
            while (true) {
                if (execPlans.size() > 0) {
                    ExecPlan execPlan = execPlans.poll();
                    assert execPlan != null;
                    String res = execPlan.execute(dataPath);
                    results.put(execPlan.hashCode(), res);
                }
            }
        });
    }

}
