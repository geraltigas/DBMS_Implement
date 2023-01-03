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
        for (int i = 0; i < 1000; i+=2 ) {
            res = clientTester.send("DELETE FROM test WHERE id = " + i);
            assertEquals("\n" +
                    "Delete 1 records from table test",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertTrue(res.contains("Total records: 500"));
        clientTester.close();
    }

    @Test
    public void testComplexDelete() throws IOException, InterruptedException {
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
        for (int i = 0; i < 200; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test where id = 199 OR id = 180");
        assertEquals("""

                |id        |name      |float     |
                |180       |name180   |180.0     |
                |199       |name199   |199.0     |
                Total records: 2""",res);
        res = clientTester.send("DELETE FROM test WHERE id = 199 AND name = 'name199'");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 199 OR id = 180");
        assertEquals("""

                |id        |name      |float     |
                |180       |name180   |180.0     |
                Total records: 1""",res);
        res = clientTester.send("SELECT * FROM test where id = 198 OR id = 180");
        assertEquals("""

                |id        |name      |float     |
                |180       |name180   |180.0     |
                |198       |name198   |198.0     |
                Total records: 2""",res);
        res = clientTester.send("DELETE FROM test WHERE id = 180 OR name = 'name198'");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 198 OR id = 180");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        res = clientTester.send("SELECT * FROM test where id = 99 OR id = 80");
        assertEquals("""

                |id        |name      |float     |
                |80        |name80    |80.0      |
                |99        |name99    |99.0      |
                Total records: 2""",res);
        res = clientTester.send("DELETE FROM test where (id = 99 AND name = 'name99') OR (id = 80 AND name = 'name80')");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 99 OR id = 80");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        res = clientTester.send("SELECT * FROM test where id = 97 OR id = 70");
        assertEquals("""

                |id        |name      |float     |
                |70        |name70    |70.0      |
                |97        |name97    |97.0      |
                Total records: 2""",res);
        res = clientTester.send("DELETE FROM test where id = 97 AND name = 'name97' OR float = 70.0");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("SELECT * FROM test where id = 97 OR id = 70");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        clientTester.close();
    }

    @Test
    public void wiredExpressTest1() throws InterruptedException, IOException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        clientTester.send("AUTH root root");
        String res;
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");
        assertEquals("""

                Create table file;
                Write table metadata;
                Write table header""",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                |0         |name0     |0.0       |
                |1         |name1     |1.0       |
                |2         |name2     |2.0       |
                Total records: 3""",res);
        res = clientTester.send("DELETE FROM test WHERE id = id");
        assertEquals("\n" +
                "Delete 3 records from table test",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
    }

    @Test
    public void wiredExpressionTest2() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                |0         |name0     |0.0       |
                |1         |name1     |1.0       |
                |2         |name2     |2.0       |
                Total records: 3""",res);
        res = clientTester.send("DELETE FROM test WHERE 1 = 1");
        assertEquals("\n" +
                "Delete 3 records from table test",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        clientTester.close();
    }

    @Test
    public void whereExpressionTypeNotMatch() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                |0         |name0     |0.0       |
                |1         |name1     |1.0       |
                |2         |name2     |2.0       |
                Total records: 3""",res);
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
        assertEquals("""

                |id        |name      |float     |
                |0         |name0     |0.0       |
                |1         |name1     |1.0       |
                |2         |name2     |2.0       |
                Total records: 3""",res);
        clientTester.close();
    }

    @Test
    public void testDeleteIndex() throws IOException, InterruptedException {
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
        clientTester.send("CREATE INDEX test_id ON test(id)");
        for (int i = 0; i < 1000; i+=2 ) {
            res = clientTester.send("DELETE FROM test WHERE id = " + i);
            assertEquals("\n" +
                    "Delete 1 records from table test",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertTrue(res.contains("Total records: 500"));
        clientTester.close();
    }

    @Test
    public void testDeleteScanWithIndex() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20),float FLOAT,age int)");
        assertEquals("""

                Create table file;
                Write table metadata;
                Write table header""",res);
        for (int i = 0; i < 1000; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ",'name" + i + "'," + i + ".0," + i + ")");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("CREATE INDEX test_id ON test(id)");
        assertEquals("\n" +
                "Create index test_id on test.id success",res);
        res = clientTester.send("CREATE INDEX test_name ON test(name)");
        assertEquals("\n" +
                "Create index test_name on test.name success",res);
        res = clientTester.send("CREATE INDEX test_float ON test(float)");
        assertEquals("\n" +
                "Create index test_float on test.float success",res);

//         delete using one index
        res = clientTester.send("DELETE FROM test WHERE id = 1");
        assertEquals("\n" +
                "Delete 1 records from table test",res);

//         delete using one index type varchar and float
        res = clientTester.send("DELETE FROM test WHERE name = 'name2'");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        res = clientTester.send("DELETE FROM test WHERE float = 3.0");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        // delete using more index
        res = clientTester.send("DELETE FROM test WHERE name = 'name4' OR id = 5");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("DELETE FROM test WHERE (name = 'name6' OR id = 7) AND float = 7.0");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        res = clientTester.send("DELETE FROM test WHERE name = 'name8' OR age = 9");
        assertEquals("\n" +
                "Delete 2 records from table test",res);
        res = clientTester.send("DELETE FROM test WHERE name = 'name10' AND age = 10");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        res = clientTester.send("DELETE FROM test WHERE name = 'name11' AND id = 11");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        res = clientTester.send("DELETE FROM test WHERE age = 12");
        assertEquals("\n" +
                "Delete 1 records from table test",res);
        clientTester.close();
    }

    @Test
    public void typeNotMatch() throws IOException, InterruptedException {
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
        res = clientTester.send("CREATE INDEX test_id ON test(id)");
        assertEquals("\n" +
                "Create index test_id on test.id success",res);
        res = clientTester.send("DELETE FROM test WHERE id = 'ok'");
        assertEquals("\n" +
                "Column id type dont match",res);
        clientTester.close();
    }
}
