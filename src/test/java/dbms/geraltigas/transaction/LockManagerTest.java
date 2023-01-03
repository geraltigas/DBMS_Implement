package dbms.geraltigas.transaction;

import dbms.geraltigas.dataccess.DiskManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LockManagerTest {

    LockManager lockManager = new LockManager();

    @Test
    void computeId() {
        System.out.println(LockManager.computeId("test", DiskManager.AccessType.TABLE, null, 1));
        System.out.println(LockManager.computeId("test", DiskManager.AccessType.INDEX, "test", 1));
    }

    @Test
    void lockRead() {

//        new Thread(() -> {
//            System.out.println("1 want lock write");
//            lockManager.lockWrite(1, 1);
//            System.out.println("1 lock write");
//            try {
//                Thread.sleep(1000);
//                System.out.println("sleep 1000");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            lockManager.unlock(1, 1);
//        }).start();
//
//        new Thread(() -> {
//            System.out.println("2 want lock write");
//            lockManager.lockWrite(1, 2);
//            System.out.println("2 lock write");
//            try {
//                Thread.sleep(1000);
//                System.out.println("sleep 1000");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            lockManager.unlock(1, 2);
//        }).start();
//
//        new Thread(() -> {
//            System.out.println("3 want lock read");
//            lockManager.lockRead(1, 3);
//            System.out.println("3 lock read");
//            try {
//                Thread.sleep(1000);
//                System.out.println("sleep 1000");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            lockManager.unlock(1, 3);
//        }).start();
//
//        while(true) {
//        }
    }

    @Test
    void upgradeLock() {
//
//        new Thread(() -> {
//            System.out.println("1 want lock read");
//            lockManager.lockRead(1, 1);
//            System.out.println("1 lock read");
//            try {
//                Thread.sleep(1000);
//                System.out.println("sleep 1000");
//                System.out.println("1 try to upgrade lock");
//                lockManager.upgradeLock(1, 1);
//                System.out.println("1 upgrade lock");
//                Thread.sleep(1000);
//                System.out.println("sleep 1000");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            lockManager.unlock(1, 1);
//        }).start();
//
//        new Thread(() -> {
//            System.out.println("2 want lock read");
//            lockManager.lockRead(1, 2);
//            System.out.println("2 lock read");
//            try {
//                Thread.sleep(1000);
//                System.out.println("sleep 1000");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            lockManager.unlock(1, 2);
//        }).start();
//
//        new Thread(() -> {
//            System.out.println("3 want lock read");
//            lockManager.lockRead(1, 3);
//            System.out.println("3 lock read");
//            try {
//                Thread.sleep(2000);
//                System.out.println("sleep 2000");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            lockManager.unlock(1, 3);
//            System.out.println("the last read : 3 unlock");
//        }).start();
//
//        while(true) {
//        }
    }
}