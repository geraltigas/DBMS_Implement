package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class InsertTest {

    @Test
    public void massiveInserTest() throws IOException, InterruptedException {
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
        res = clientTester.send("SELECT * FROM test");
        assertEquals(true,res.contains("Total records: 1000"));
        clientTester.close();
    }

    @Test
    public void outOfRangeTest() throws IOException, InterruptedException {
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
        // int out of range
        res = clientTester.send("INSERT INTO test VALUES (12312312312312312312312312, '1',1.0)");
        assertEquals("\n" +
                "Value Format Exception, please insert valid values. \n" +
                "For input string: \"12312312312312312312312312\"",res);
        // float out of range
        res = clientTester.send("INSERT INTO test VALUES (1, '1',123123123123123123121123123123123123123123123213213312312.0)");
        assertEquals("\n" +
                "Value Format Exception, please insert valid values. \n" +
                "For input string: \"123123123123123123121123123123123123123123123213213312312.0\"",res);
        // varchar out of range
        res = clientTester.send("INSERT INTO test VALUES (1, '12312312312312312312312312123123123',1.0)");
        assertEquals("\n" +
                "Value Format Exception, please insert valid values. \n" +
                "String length too long.",res);
        // normal
        res = clientTester.send("INSERT INTO test VALUES (1, '1',1.0)");
        assertEquals("\n" +
                "Table test insert 1 records",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|1         |1         |1.0       |\n" +
                "Total records: 1",res);
        clientTester.close();
    }

    @Test
    public void insertWithColumnName() throws IOException, InterruptedException {
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
            res = clientTester.send("INSERT INTO test (name,float,id) VALUES ('name" + i + "'," + i + ".0," + i + ")");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals(true,res.contains("Total records: 1000"));
        clientTester.close();
    }

    @Test
    public void insertColumnNotMatch() throws IOException, InterruptedException {
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
        res = clientTester.send("INSERT INTO test (name,id) VALUES ('OKK',2)");
        assertEquals("\n" +
                "Field float not found in value list",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        clientTester.close();
    }

    @Test
    public void insertTypeNotMatch() throws IOException, InterruptedException { // TODO: this should do to all the other exec
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
        res = clientTester.send("INSERT INTO test (name,id,float) VALUES (1.0,'id',1)");
        assertEquals("\n" +
                "Value Format Exception, please insert valid values. \n" +
                "For input string: \"'id'\"",res);
        res = clientTester.send("INSERT INTO test (name,id,float) VALUES (1.0,1,1)");
        assertEquals("\n" +
                "Value Format Exception, please insert valid values. \n" +
                "Varchar value must be in ''",res);
        res = clientTester.send("INSERT INTO test (name,id,float) VALUES ('1.0',1,'1')");
        assertEquals("\n" +
                "Value Format Exception, please insert valid values. \n" +
                "For input string: \"'1'\"",res);
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "Total records: 0",res);
        clientTester.close();
    }

    @Test
    public void insertWithIndex() { // TODO: implement this test

    }
}
