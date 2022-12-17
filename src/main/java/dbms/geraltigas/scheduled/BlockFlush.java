package dbms.geraltigas.scheduled;

import dbms.geraltigas.buffer.BlockBuffer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BlockFlush {

    @Autowired
    BlockBuffer blockBuffer;

    @Scheduled(fixedRate = 10000)
    public void flush() {
        System.out.println("Starting to flush written page into disk");
        try {
            blockBuffer.FlushIntoDisk();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Flush finished");
    }
}
