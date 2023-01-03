package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class Test1 {
    @Test
    public void test1() {
        Thread thread = new Thread(() -> {
            try {
                ClientTester.begin();
                ClientTester clientTester = new ClientTester("RootCLient");
                clientTester.send("AUTH root root");
                clientTester.send("CREATE TABLE test1 (id INT, name VARCHAR(255))");
                clientTester.send("INSERT INTO test1 VALUES (1, 'test1')");
                clientTester.send("SELECT * FROM test1");
                clientTester.send("DROP TABLE test1");
                clientTester.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        // wait the thread to finish
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
