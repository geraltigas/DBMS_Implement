package dbms.geraltigas.transaction;

import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.utils.Printer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.server.Ssl;
import org.springframework.stereotype.Component;

import javax.swing.*;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class LockManager { // add read or write lock to every page

    ReentrantLock mutex = new ReentrantLock();
    Map<Long, PageLockInfo> lockMap = new TreeMap<>();

    @Setter
    @Getter
    class PageLockInfo {
        LockType lockType;
        int lockNum;
        List<Long> txnIdList = new ArrayList<>();
        Condition condition;
    }

    @Setter
    ExecuteEngine executeEngine;

    enum LockType {
        READ, WRITE
    }

//    @Autowired
//    ExecuteEngine executeEngine;

    public void lockRead(long pageId, long transactionId) throws InterruptedException {
        mutex.lock();
        Printer.print("Try to lockRead : " +  pageId,transactionId);
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.lockType == LockType.WRITE) {
                Printer.print("Page " + pageId + " has been lock write",transactionId);
                if (pageLockInfo.txnIdList.contains(transactionId)) {
                    Printer.print("Page " + pageId + " has been lock write by this transaction",transactionId);
                    pageLockInfo.lockNum++;
                    pageLockInfo.txnIdList.add(transactionId);
                } else {
                    Printer.print("Page " + pageId + " has been lock write by other transaction, thread stalling",transactionId);
                    pageLockInfo.condition.await();
                    // wake up
                    Printer.print(""+executeEngine.transactions.size(),"info");
                    Printer.print("Wake threadId" + transactionId,"info");
                    Printer.print("existExecutor" + executeEngine.existExecutor(transactionId),"info");if (executeEngine.existExecutor(transactionId)) {
                        Printer.print("Page " + pageId + " has been lock write by other transaction, thread wake up",transactionId);
                        lockRead(pageId, transactionId);
                    } else {
                        Printer.print("Page " + pageId + " has been lock write by other transaction, thread wake up, but transaction is aborted",transactionId);
                        mutex.unlock();
                        return;
                    }
//                        Printer.print("Page " + pageId + " has been lock write by other transaction, thread wake up",transactionId);
//                        lockRead(pageId, transactionId);
                }
            } else {
                Printer.print("Page " + pageId + " has been lock read",transactionId);
                pageLockInfo.lockNum++;
                pageLockInfo.txnIdList.add(transactionId);
                Printer.print("Add readLock success",transactionId);
            }
        } else {
            Printer.print("Page " + pageId + " has not been lock",transactionId);
            PageLockInfo pageLockInfo = new PageLockInfo();
            pageLockInfo.lockType = LockType.READ;
            pageLockInfo.lockNum = 1;
            pageLockInfo.condition = mutex.newCondition();
            pageLockInfo.txnIdList.clear();
            pageLockInfo.txnIdList.add(transactionId);
            lockMap.put(pageId, pageLockInfo);
            Printer.print("Add readLock success",transactionId);
        }
        mutex.unlock();
    }

    public void lockWrite(long pageId, long transactionId) throws InterruptedException {
        mutex.lock();
        Printer.print("Try to lockWrite : " +  pageId,transactionId);
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.lockType == LockType.WRITE) {
                Printer.print("Page " + pageId + " has been lock write",transactionId);
                if (pageLockInfo.txnIdList.contains(transactionId)) {
                    pageLockInfo.lockNum++;
                    pageLockInfo.txnIdList.add(transactionId);
                    Printer.print("Add writeLock success",transactionId);
                } else {
                    Printer.print("Page " + pageId + " has been lock write by other transaction"+ pageLockInfo.txnIdList.get(0) +", thread stalling",transactionId);
                    pageLockInfo.condition.await();
                    Printer.print(""+executeEngine.transactions.size(),"info");
                    Printer.print("Wake threadId" + transactionId,"info");
                    Printer.print("existExecutor" + executeEngine.existExecutor(transactionId),"info");
                    if (executeEngine.existExecutor(transactionId)) {
                        Printer.print("Page " + pageId + " has been lock write by other transaction, thread wake up, try to lock again",transactionId);
                        lockWrite(pageId, transactionId);
                    } else {
                        Printer.print("Page " + pageId + " has been lock write by other transaction, thread wake up, but transaction is aborted",transactionId);
                        mutex.unlock();
                        throw new InterruptedException();
                    }
//                        Printer.print("Page " + pageId + " has been lock write by other transaction, thread wake up, try to lock again",transactionId);
//                        lockWrite(pageId, transactionId);
                }
            } else {
                boolean allMine = true;
                for (Long txnId : pageLockInfo.txnIdList) {
                    if (txnId != transactionId) {
                        allMine = false;
                        break;
                    }
                }
                if (allMine) {
                    pageLockInfo.lockType = LockType.WRITE;
                    pageLockInfo.lockNum++;
                    pageLockInfo.txnIdList.add(transactionId);
                    Printer.print("Page " + pageId + " has been lock read by this transaction, lock updated",transactionId);
                } else {
                    Printer.print("Page " + pageId + " has been lock read by other transaction, thread stalling",transactionId);
                    pageLockInfo.condition.await();
                    Printer.print(""+executeEngine.transactions.size(),"info");
                    Printer.print("Wake threadId" + transactionId,"info");
                    Printer.print("existExecutor" + executeEngine.existExecutor(transactionId),"info");if (executeEngine.existExecutor(transactionId)) {
                        Printer.print("Page " + pageId + " has been lock read by other transaction, thread wake up, try to lock again",transactionId);
                        lockWrite(pageId, transactionId);
                    } else {
                        Printer.print("Page " + pageId + " has been lock read by other transaction, thread wake up, but transaction is aborted",transactionId);
                        mutex.unlock();
                        throw new InterruptedException();
                    }
//                        lockWrite(pageId, transactionId);
                }
            }
        } else {
            Printer.print("Page " + pageId + " has not been lock",transactionId);
            PageLockInfo pageLockInfo = new PageLockInfo();
            pageLockInfo.lockType = LockType.WRITE;
            pageLockInfo.lockNum = 1;
            pageLockInfo.condition = mutex.newCondition();
            pageLockInfo.txnIdList.add(transactionId);
            lockMap.put(pageId, pageLockInfo);
            Printer.print("Add writeLock success",transactionId);
        }
        mutex.unlock();
    }

    private void unlock(long pageId, long transactionId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.txnIdList.contains(transactionId)) {
                pageLockInfo.lockNum--;
                pageLockInfo.txnIdList.remove(transactionId);
                Printer.print("Unlock one lock of "+pageId,transactionId);
                if (pageLockInfo.lockNum == 0) {
                    lockMap.remove(pageId);
                    pageLockInfo.condition.signalAll();
                    Printer.print("Unlock all lock success",transactionId);
                }
            }
        }
        mutex.unlock();
    }

    public void unlockAll(long transactionId) {
        mutex.lock();
        List<Long> pageIdList = new ArrayList<>();
        for (Map.Entry<Long, PageLockInfo> entry : lockMap.entrySet()) {
            for (Long txnId : entry.getValue().txnIdList) {
                if (txnId == transactionId) {
                    pageIdList.add(entry.getKey());
                }
            }
        }
        for (Long pageId : pageIdList) {
            unlock(pageId, transactionId);
        }
        Printer.print("Unlock all pages",transactionId);
        mutex.unlock();
    }

    @Deprecated
    private void upgradeLock(long pageId, long transactionId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            if (pageLockInfo.txnIdList.contains(transactionId)) {
                pageLockInfo.lockNum--;
                if (pageLockInfo.lockNum == 0) {
                    pageLockInfo.lockType = LockType.WRITE;
                    pageLockInfo.lockNum = 1;
                }else {
                    try {
                        pageLockInfo.condition.await();
                        upgradeLock(pageId, transactionId);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        mutex.unlock();
    }

    private LockType getLockType(long pageId) {
        mutex.lock();
        if (lockMap.containsKey(pageId)) {
            PageLockInfo pageLockInfo = lockMap.get(pageId);
            mutex.unlock();
            return pageLockInfo.lockType;
        }
        mutex.unlock();
        return null;
    }

    public static long computeId(String tableName, DiskManager.AccessType type,String appendName, int pageIndex) {
        System.out.println("[LockManger] computeId: \n tableName: " + tableName + ", \naccessType: " + type + ", \nappendName: " + appendName + ", \nPageIndex: " + pageIndex);
        // compute id from table name, access type, append name and page number
        long id = tableName.hashCode();
        id<<=32;
        id = id * 100 + type.ordinal();
        id = id * 100 + ((appendName == null) ? 0 :appendName.hashCode());
        id = id * 100 + pageIndex;
        return id;
    }
}
