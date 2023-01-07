package dbms.geraltigas.scheduled;

import dbms.geraltigas.buffer.PageBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.dataccess.ExecuteEngine;
import jdk.jfr.Threshold;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

import static dbms.geraltigas.utils.Printer.DEBUG;

@Component
public class Tasks {

    @Autowired
    PageBuffer pageBuffer;

    @Autowired
    ExecuteEngine executeEngine;

    @Autowired
    private DiskManager diskManager;

    public AtomicBoolean openClearHoles = new AtomicBoolean(true);

//    @Scheduled(fixedRate = 1000 * 5)
//    private void flush() {
//        System.out.println("[DataPersistence] Starting to flush written page into disk");
//        try {
//            if (!executeEngine.hasTxn()) {
//                executeEngine.setNormalStop(true);
//                System.out.println("[DataPersistence] NormalExecutor stopped");
//                Thread.sleep(1000);
//                pageBuffer.FlushIntoDisk();
//                executeEngine.setNormalStop(false);
//            }else{
//                System.out.println("[DataPersistence] There are still transactions running, flush postponed");
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("[DataPersistence] Flush finished");
//    }

    @Scheduled(fixedRate = 1000 * 63)
    public void deadLockDetect() {
        System.out.println("[DeadLockDetect] Starting to detect dead lock");
        DEBUG("HERE 1");
        try {
            DEBUG("here");
            DEBUG("has txn: "+executeEngine.hasTxn());
            if (executeEngine.hasTxn()) {
                DEBUG("into deadlock detect");
                executeEngine.detectDeadLock();
            }else{
                System.out.println("[DeadLockDetect] There is no transaction running, dead lock detect postponed");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[DeadLockDetect] Detect finished");
    }

    @Scheduled(fixedRate = 1000 * (60 + 2))
    private void clearHoles() {
        System.out.println("[HoleClear] Starting to clear holes");
        if (!openClearHoles.get()) {
            System.out.println("[HoleClear] HoleClear is disabled now");
            return;
        }
        try {
            if (!executeEngine.hasTxn()) {
                executeEngine.setNormalStop(true);
                Thread.sleep(1000);
                System.out.println("[HoleClear] Clearing holes");
                diskManager.clearHoles();
                executeEngine.setNormalStop(false);
            }else{
                System.out.println("[HoleClear] There are still transactions running, skip this time");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[HoleClear] Clear finished");
    }
}
