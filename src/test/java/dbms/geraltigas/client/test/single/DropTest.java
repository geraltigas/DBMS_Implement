package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;
@SpringBootTest
public class DropTest {
    @Test
    public void dropTableTest() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        clientTester.send("AUTH root root");
        String res = "";
        // normal drop
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");

        assertEquals("""

                Create table file;
                Write table metadata;
                Write table header""",res);

        res = clientTester.send("DROP TABLE test");
        assertEquals("""

                Table file test dropped;
                Meta test deleted;
                Table test dropped""",res);
        // drop done exist table
        res = clientTester.send("DROP TABLE test");
        assertEquals("\n" +
                "Table test not exists",res);
        clientTester.close();
    }
}
