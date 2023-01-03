package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class DeleteTest {
    @Test
    public void testMassiveDelete() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");
        assertEquals("\n" +
                "Create table file;\n" +
                "Write table metadata;\n" +
                "Write table header",res);
        for (int i = 0; i < 1000; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        for (int i = 0; i < 1000; i+=2 ) {
            res = clientTester.send("DELETE FROM test WHERE id = " + i);
            assertEquals("\n" +
                    "Delete 1 records from table test",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals(true,res.contains("Total records: 500"));
        clientTester.close();
    }

    @Test
    public void testComplexDelete() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");
        assertEquals("\n" +
                "Create table file;\n" +
                "Write table metadata;\n" +
                "Write table header",res);
        for (int i = 0; i < 200; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test where id = 199 OR id = 180");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|180       |name180   |180.0     |\n" +
                "|199       |name199   |199.0     |\n" +
                "Total records: 2",res);
        res = clientTester.send("DELETE FROM test WHERE id = 199 AND name = 'name199'");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 199 OR id = 180");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|180       |name180   |180.0     |\n" +
                "Total records: 1",res);
        res = clientTester.send("SELECT * FROM test where id = 198 OR id = 180");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|180       |name180   |180.0     |\n" +
                "|198       |name198   |198.0     |\n" +
                "Total records: 2",res);
        res = clientTester.send("DELETE FROM test WHERE id = 180 OR name = 'name198'");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 198 OR id = 180");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        res = clientTester.send("SELECT * FROM test where id = 99 OR id = 80");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|80        |name80    |80.0      |\n" +
                "|99        |name99    |99.0      |\n" +
                "Total records: 2",res);
        res = clientTester.send("DELETE FROM test where (id = 99 AND name = 'name99') OR (id = 80 AND name = 'name80')");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 99 OR id = 80");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        res = clientTester.send("SELECT * FROM test where id = 97 OR id = 70");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|70        |name70    |70.0      |\n" +
                "|97        |name97    |97.0      |\n" +
                "Total records: 2",res);
        res = clientTester.send("DELETE FROM test where id = 97 AND name = 'name97' OR float = 70.0");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 97 OR id = 70");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        clientTester.close();
    }

    @Test
    public void wiredExpressTest1() throws InterruptedException, IOException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        clientTester.send("AUTH root root");
        String res = "";
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");
        assertEquals("\n" +
                "Create table file;\n" +
                "Write table metadata;\n" +
                "Write table header",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|0         |name0     |0.0       |\n" +
                "|1         |name1     |1.0       |\n" +
                "|2         |name2     |2.0       |\n" +
                "Total records: 3",res);
        res = clientTester.send("DELETE FROM test WHERE id = id");
        assertEquals("\n" +
                "Delete 3 records from table test",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
    }

    @Test
    public void wiredExpressionTest2() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|0         |name0     |0.0       |\n" +
                "|1         |name1     |1.0       |\n" +
                "|2         |name2     |2.0       |\n" +
                "Total records: 3",res);
        res = clientTester.send("DELETE FROM test WHERE 1 = 1");
        assertEquals("\n" +
                "Delete 3 records from table test",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        clientTester.close();
    }

    @Test
    public void whereExpressionTypeNotMatch() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|0         |name0     |0.0       |\n" +
                "|1         |name1     |1.0       |\n" +
                "|2         |name2     |2.0       |\n" +
                "Total records: 3",res);
        res = clientTester.send("DELETE FROM test WHERE id = 'ok'");
        assertEquals("\n" +
                "Integer type can not compare with other type",res);
        res = clientTester.send("DELETE FROM test WHERE name = 1");
        assertEquals("\n" +
                "String type can not compare with other type",res);
        res = clientTester.send("DELETE FROM test WHERE float = 'ok'");
        assertEquals("\n" +
                "Float type can not compare with other type",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|0         |name0     |0.0       |\n" +
                "|1         |name1     |1.0       |\n" +
                "|2         |name2     |2.0       |\n" +
                "Total records: 3",res);
        clientTester.close();
    }

    @Test
    public void testDeleteWithIndex() { // TODO: implement this test

    }
}
