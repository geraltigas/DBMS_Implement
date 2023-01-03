package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class InsertTest {

    @Test
    public void massiveInsertTest() throws IOException, InterruptedException {
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
        res = clientTester.send("SELECT * FROM test");
        assertTrue(res.contains("Total records: 1000"));
        clientTester.close();
    }

    @Test
    public void outOfRangeTest() throws IOException, InterruptedException {
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
        // int out of range
        res = clientTester.send("INSERT INTO test VALUES (12312312312312312312312312, '1',1.0)");
        assertEquals("""

                Value Format Exception, please insert valid values.\s
                For input string: "12312312312312312312312312\"""",res);
        // float out of range
        res = clientTester.send("INSERT INTO test VALUES (1, '1',123123123123123123121123123123123123123123123213213312312.0)");
        assertEquals("""

                Value Format Exception, please insert valid values.\s
                For input string: "123123123123123123121123123123123123123123123213213312312.0\"""",res);
        // varchar out of range
        res = clientTester.send("INSERT INTO test VALUES (1, '12312312312312312312312312123123123',1.0)");
        assertEquals("""

                Value Format Exception, please insert valid values.\s
                String length too long.""",res);
        // normal
        res = clientTester.send("INSERT INTO test VALUES (1, '1',1.0)");
        assertEquals("\n" +
                "Table test insert 1 records",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                |1         |1         |1.0       |
                Total records: 1""",res);
        clientTester.close();
    }

    @Test
    public void insertWithColumnName() throws IOException, InterruptedException {
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
            res = clientTester.send("INSERT INTO test (name,float,id) VALUES ('name" + i + "'," + i + ".0," + i + ")");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertTrue(res.contains("Total records: 1000"));
        clientTester.close();
    }

    @Test
    public void insertColumnNotMatch() throws IOException, InterruptedException {
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
        res = clientTester.send("INSERT INTO test (name,id) VALUES ('OKK',2)");
        assertEquals("\n" +
                "Field float not found in value list",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        clientTester.close();
    }

    @Test
    public void insertTypeNotMatch() throws IOException, InterruptedException {
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
        res = clientTester.send("INSERT INTO test (name,id,float) VALUES (1.0,'id',1)");
        assertEquals("""

                Value Format Exception, please insert valid values.\s
                For input string: "'id'\"""",res);
        res = clientTester.send("INSERT INTO test (name,id,float) VALUES (1.0,1,1)");
        assertEquals("""

                Value Format Exception, please insert valid values.\s
                Varchar value must be in ''""",res);
        res = clientTester.send("INSERT INTO test (name,id,float) VALUES ('1.0',1,'1')");
        assertEquals("""

                Value Format Exception, please insert valid values.\s
                For input string: "'1'\"""",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |name      |float     |
                Total records: 0""",res);
        clientTester.close();
    }

    @Test
    public void insertWithIndex() throws InterruptedException, IOException {
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
        res = clientTester.send("CREATE INDEX test_id_index ON test(id)");
        assertEquals("\n" +
                "Create index test_id_index on test.id success",res);
        res = clientTester.send("CREATE INDEX test_id_name ON test(name)");
        assertEquals("\n" +
                "Create index test_id_name on test.name success",res);
        for (int i = 0; i < 1000; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertTrue(res.contains("Total records: 1000"));

        res = clientTester.send("SELECT * FROM test WHERE id = 1 OR name = 'name100'");
        assertEquals("""

                |id        |name      |float     |
                |1         |name1     |1.0       |
                |100       |name100   |100.0     |
                Total records: 2""",res);
        res = clientTester.send("SELECT * FROM test WHERE id = 1 AND name = 'name1'");
        assertEquals("""

                |id        |name      |float     |
                |1         |name1     |1.0       |
                Total records: 1""",res);
        res = clientTester.send("SELECT * FROM test WHERE id = 1");
        assertEquals("""

                |id        |name      |float     |
                |1         |name1     |1.0       |
                Total records: 1""",res);
        clientTester.close();
    }
}
