package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

@SpringBootTest
public class ClearHolesTest {

    public void test() throws IOException, InterruptedException {
        Thread thread = new Thread(() -> {
            try {
                ClientTester.begin();
                ClientTester clientTester = new ClientTester("RootCLient");
                clientTester.clearAllData();
                clientTester.send("AUTH root root");
                clientTester.send("CREATE TABLE test1 (id INT, name VARCHAR(255))");

                for (int i = 0; i < 100; i++) {
                    clientTester.send("INSERT INTO test1 VALUES (" + i + ", 'test1')");
                }
                clientTester.send("CREATE INDEX test1_id ON test1 (id)");

                for (int i = 0; i < 100; i+=2) {
                    clientTester.send("DELETE FROM test1 WHERE id = " + i);
                }
                clientTester.send("SELECT * FROM test1");

                while(true) {
                    Thread.sleep(1000);
                    clientTester.send("SELECT * FROM test1 where id = 1");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        // make sure this set run 2 mins
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                thread.interrupt();
            }
        }, 1000 * (60 + 10));

        // wait until the thread finish
        thread.start();
        thread.join();

    }
}
