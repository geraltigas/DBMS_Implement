package dbms.geraltigas.transaction;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class LockManager { // add read or write lock to every page
    Map<Long, ReentrantReadWriteLock> lockMap = new TreeMap<>();
    synchronized public ReentrantReadWriteLock getLock(long id) {
        if (!lockMap.containsKey(id)) {
            lockMap.put(id, new ReentrantReadWriteLock());
        }
        return lockMap.get(id);
    }

    synchronized public void deleteLock(long id) {
        lockMap.remove(id);
    }
}
