package dbms.geraltigas.scheduled;

import dbms.geraltigas.buffer.PageBuffer;
import dbms.geraltigas.dataccess.ExecuteEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BlockFlush {

    @Autowired
    PageBuffer pageBuffer;

    @Autowired
    ExecuteEngine executeEngine;

    @Scheduled(fixedRate = 10000)
    public void flush() {
        System.out.println("[DataPersistence] Starting to flush written page into disk");
        try {
            pageBuffer.FlushIntoDisk();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[DataPersistence] Flush finished");
    }

//    @Scheduled(fixedRate = 10000)
//    public void deadLockDetect() {
//        System.out.println("[DeadLockDetect] Starting to detect dead lock");
//        try {
//            executeEngine.detectDeadLock();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("[DeadLockDetect] Detect finished");
//    }
}
