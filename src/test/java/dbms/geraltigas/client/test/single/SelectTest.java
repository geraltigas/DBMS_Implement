package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class SelectTest {
    @Test
    public void massiveSelectTest() throws IOException, InterruptedException {
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
        res = clientTester.send("SELECT * FROM test");
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
    public void complexSelectTest() throws IOException, InterruptedException {
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
        res = clientTester.send("SELECT * FROM test WHERE id = 199 AND name = 'name199'");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|199       |name199   |199.0     |\n" +
                "Total records: 1",res);
        res = clientTester.send("SELECT * FROM test where id = 198 OR id = 180");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|180       |name180   |180.0     |\n" +
                "|198       |name198   |198.0     |\n" +
                "Total records: 2",res);
        res = clientTester.send("SELECT * FROM test WHERE id = 180 OR name = 'name198'");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|180       |name180   |180.0     |\n" +
                "|198       |name198   |198.0     |\n" +
                "Total records: 2",res);
        res = clientTester.send("SELECT * FROM test where id = 99 OR id = 80");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|80        |name80    |80.0      |\n" +
                "|99        |name99    |99.0      |\n" +
                "Total records: 2",res);
        res = clientTester.send("SELECT * FROM test where (id = 99 AND name = 'name99') OR (id = 80 AND name = 'name80')");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|80        |name80    |80.0      |\n" +
                "|99        |name99    |99.0      |\n" +
                "Total records: 2",res);
        res = clientTester.send("SELECT * FROM test where id = 97 OR id = 70");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|70        |name70    |70.0      |\n" +
                "|97        |name97    |97.0      |\n" +
                "Total records: 2",res);
        res = clientTester.send("SELECT * FROM test where id = 97 AND name = 'name97' OR float = 70.0");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|70        |name70    |70.0      |\n" +
                "|97        |name97    |97.0      |\n" +
                "Total records: 2",res);
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
        res = clientTester.send("SELECT * FROM test WHERE id = id");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|0         |name0     |0.0       |\n" +
                "|1         |name1     |1.0       |\n" +
                "|2         |name2     |2.0       |\n" +
                "Total records: 3",res);
        clientTester.close();
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
        res = clientTester.send("SELECT * FROM test WHERE 1 = 1");
        assertEquals("\n" +
                "|id        |name      |float     |\n" +
                "|0         |name0     |0.0       |\n" +
                "|1         |name1     |1.0       |\n" +
                "|2         |name2     |2.0       |\n" +
                "Total records: 3",res);
        clientTester.close();
    }

    @Test
    public void wiredExpressionTest3() throws IOException, InterruptedException {
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
        res = clientTester.send("SELECT FROM test");
        assertEquals("\n" +
                "Sql Syntax Error",res);
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
        res = clientTester.send("SELECT * FROM test WHERE id = 'ok'");
        assertEquals("\n" +
                "Integer type can not compare with other type",res);
        res = clientTester.send("SELECT * FROM test WHERE name = 1");
        assertEquals("\n" +
                "String type can not compare with other type",res);
        res = clientTester.send("SELECT * FROM test WHERE float = 'ok'");
        assertEquals("\n" +
                "Float type can not compare with other type",res);
        clientTester.close();
    }

    @Test
    public void testSelectedColumnNameNotMatch() throws IOException, InterruptedException {
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

        res = clientTester.send("SELECT id,name,age FROM test WHERE id = 1");
        assertEquals("\n" +
                "Field age not found",res);
        res = clientTester.send("SELECT id,name,age FROM test");
        assertEquals("\n" +
                "Field age not found",res);

        clientTester.close();
    }

    @Test
    public void testExpressionInSelect() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |age       |name      |float     |\n" +
                "|0         |1         |name0     |0.0       |\n" +
                "|1         |2         |name1     |1.0       |\n" +
                "|2         |3         |name2     |2.0       |\n" +
                "Total records: 3",res);
        res = clientTester.send("SELECT id - age AS delta FROM test WHERE id = 1");
        assertEquals("\n" +
                "|delta     |\n" +
                "|-1        |\n" +
                "Total records: 1",res);
        res = clientTester.send("SELECT id + age AS total FROM test WHERE id = 1");
        assertEquals("\n" +
                "|total     |\n" +
                "|3         |\n" +
                "Total records: 1",res);
        res = clientTester.send("SELECT id + age AS total FROM test");
        assertEquals("\n" +
                "|total     |\n" +
                "|1         |\n" +
                "|3         |\n" +
                "|5         |\n" +
                "Total records: 3",res);
        clientTester.close();
    }

    @Test
    public void expressionWithAliasNoWhere() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test");
        assertEquals("\n" +
                "|total     |delta1    |\n" +
                "|1         |2         |\n" +
                "|3         |5         |\n" +
                "|5         |8         |\n" +
                "Total records: 3",res);
        res = clientTester.send("SELECT total + age AS delta1,id + age AS total FROM test");

        clientTester.close();
    }

    @Test
    public void expressionWithAlias() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test WHERE id = 1");
        assertEquals("\n" +
                "|total     |delta1    |\n" +
                "|3         |5         |\n" +
                "Total records: 1",res);
    }

    @Test
    public void expressionAlrithmaticError() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT id + name AS total,total + age AS delta1 FROM test WHERE id = 1");
        assertEquals("\n" +
                "Integer type can not be plused with a non-numeric type",res);
        res = clientTester.send("SELECT id - float AS total FROM test WHERE id = 1");
        assertEquals("\n" +
                "|total     |\n" +
                "|0.0       |\n" +
                "Total records: 1",res);
        res = clientTester.send("SELECT id + float AS total FROM test WHERE id = 1");
        assertEquals("\n" +
                "|total     |\n" +
                "|2.0       |\n" +
                "Total records: 1",res);
        clientTester.close();
    }

    @Test
    public void multiTableSelect() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("CREATE TABLE test2 (id2 INT,age2 int, name2 VARCHAR(20),float2 FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test2 VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test2 insert 1 records",res);
        }
        res = clientTester.send("SELECT id,id2 FROM test,test2 WHERE id = id2");
        assertEquals("\n" +
                "|id        |id2       |\n" +
                "|0         |0         |\n" +
                "|1         |1         |\n" +
                "|2         |2         |\n" +
                "Total records: 3",res);
        res = clientTester.send("SELECT id,id2 FROM test,test2 WHERE id = id2 AND id = 1");
        assertEquals("\n" +
                "|id        |id2       |\n" +
                "|1         |1         |\n" +
                "Total records: 1",res);
        clientTester.close();
    }

    @Test
    public void multiTableSelectDuplicateColumnName() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res = "";
        clientTester.send("AUTH root root");
        res = clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("\n" +
                "|id        |age       |name      |float     |\n" +
                "|0         |1         |name0     |0.0       |\n" +
                "|1         |2         |name1     |1.0       |\n" +
                "|2         |3         |name2     |2.0       |\n" +
                "Total records: 3",res);
        res = clientTester.send("CREATE TABLE test2 (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test2 VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test2 insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test2");
        assertEquals("\n" +
                "|id        |age       |name      |float     |\n" +
                "|0         |1         |name0     |0.0       |\n" +
                "|1         |2         |name1     |1.0       |\n" +
                "|2         |3         |name2     |2.0       |\n" +
                "Total records: 3",res);
        res = clientTester.send("SELECT * FROM test2,test");
        assertEquals("\n" +
                "Ambiguous column name",res);
        res = clientTester.send("SELECT id,id2 FROM test,test2 WHERE id = id2");
        assertEquals("\n" +
                "Ambiguous column name",res);
        res = clientTester.send("SELECT id,id2 FROM test,test2 WHERE id = id2 AND id = 1");
        assertEquals("\n" +
                "Ambiguous column name",res);
        clientTester.close();
    }

    // TODO: test index
}
