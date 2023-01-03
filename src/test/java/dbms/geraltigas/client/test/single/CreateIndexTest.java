package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class CreateIndexTest {

    @Test
    public void test() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");

        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");
        assertEquals("""

                Create table file;
                Write table metadata;
                Write table header""",res);
        for (int i = 0; i < 1000; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        clientTester.send("CREATE INDEX test_id_index ON test(id)");
        res = clientTester.send("SELECT * FROM test");
        assertTrue(res.contains("Total records: 1000"));
        clientTester.close();
    }

    @Test
    public void createNotValid() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");

        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");
        assertEquals("""

                Create table file;
                Write table metadata;
                Write table header""",res);
        for (int i = 0; i < 1000; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        // create twice
        res = clientTester.send("CREATE INDEX test_id_index ON test(id)");
        assertEquals("\n" +
                "Create index test_id_index on test.id success",res);
        res = clientTester.send("CREATE INDEX test_id_index ON test(id)");
        assertEquals("\n" +
                "Index already exists",res);
        // create not exist table
        res = clientTester.send("CREATE INDEX test_id_index ON test1(id)");
        assertEquals("\n" +
                "Table test1 does not exist",res);
        // create not exist column
        res = clientTester.send("CREATE INDEX test_id_index1 ON test(id1)");
        assertEquals("\n" +
                "Column test_id_index1 does not exist",res);
        clientTester.close();
    }

}
