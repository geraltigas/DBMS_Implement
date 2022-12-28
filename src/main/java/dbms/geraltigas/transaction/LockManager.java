package dbms.geraltigas.transaction;

import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.format.tables.TableDefine;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class LockManager { // add read or write lock to every page

    @Setter
    @Getter
    class PageLockInfo {
        LockType lockType;
        int lockNum;
        long transactionId;
        Condition condition;
    }

    ReentrantLock mutex = new ReentrantLock();

    enum LockType {
        READ, WRITE
    }

    Map<Long, PageLockInfo> lockMap = new TreeMap<>();

    public void lockRead(long pageId, long transactionId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.lockType == LockType.WRITE) {
                if (pageLockInfo.transactionId == transactionId) {
                    pageLockInfo.lockNum++;
                } else {
                    try {
                        pageLockInfo.condition.await();
                        lockRead(pageId, transactionId);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                pageLockInfo.lockNum++;
            }
        } else {
            PageLockInfo pageLockInfo = new PageLockInfo();
            pageLockInfo.lockType = LockType.READ;
            pageLockInfo.lockNum = 1;
            pageLockInfo.condition = mutex.newCondition();
            pageLockInfo.transactionId = transactionId;
            lockMap.put(pageId, pageLockInfo);
        }
        mutex.unlock();
    }

    public void lockWrite(long pageId, long transactionId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.lockType == LockType.WRITE) {
                if (pageLockInfo.transactionId == transactionId) {
                    pageLockInfo.lockNum++;
                } else {
                    try {
                        pageLockInfo.condition.await();
                        lockWrite(pageId, transactionId);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                try {
                    pageLockInfo.condition.await();
                    lockWrite(pageId, transactionId);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            PageLockInfo pageLockInfo = new PageLockInfo();
            pageLockInfo.lockType = LockType.WRITE;
            pageLockInfo.lockNum = 1;
            pageLockInfo.condition = mutex.newCondition();
            pageLockInfo.transactionId = transactionId;
            lockMap.put(pageId, pageLockInfo);
        }
        mutex.unlock();
    }

    public void unlock(long pageId, long transactionId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.transactionId == transactionId) {
                pageLockInfo.lockNum--;
                if (pageLockInfo.lockNum == 0) {
                    lockMap.remove(pageId);
                    pageLockInfo.condition.signalAll();
                }
            }
        }
        mutex.unlock();
    }

    public void unlockAll(long transactionId) {
        mutex.lock();
        List<Long> pageIdList = new ArrayList<>();
        for (Map.Entry<Long, PageLockInfo> entry : lockMap.entrySet()) {
            if (entry.getValue().transactionId == transactionId) {
                pageIdList.add(entry.getKey());
            }
        }
        for (Long pageId : pageIdList) {
            unlock(pageId, transactionId);
        }
        mutex.unlock();
    }

    public void updateLock(long pageId, long transactionId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.transactionId == transactionId) {
                pageLockInfo.lockType = LockType.WRITE;
            }
        }
        mutex.unlock();
    }

    public LockType getLockType(long pageId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            mutex.unlock();
            return pageLockInfo.lockType;
        }
        mutex.unlock();
        return null;
    }

    public static long computeId(String tableName, DiskManager.AccessType type,String appendName, int pageNum) {
        // compute id from table name, access type, append name and page number
        long id = tableName.hashCode();
        id<<=32;
        id = id * 100 + type.ordinal();
        id = id * 100 + ((appendName == null) ? 0 :appendName.hashCode());
        id = id * 100 + pageNum;
        return id;
    }
}
