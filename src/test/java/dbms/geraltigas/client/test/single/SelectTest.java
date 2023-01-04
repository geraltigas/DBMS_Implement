package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class SelectTest {
    @Test
    public void massiveSelectTest() throws IOException, InterruptedException {
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
        clientTester.send("SELECT * FROM test");
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
    public void complexSelectTest() throws IOException, InterruptedException {
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
        res = clientTester.send("SELECT * FROM test WHERE id = 199 AND name = 'name199'");
        assertEquals("""

                |id        |name      |float     |
                |199       |name199   |199.0     |
                Total records: 1""",res);
        res = clientTester.send("SELECT * FROM test where id = 198 OR id = 180");
        assertEquals("""

                |id        |name      |float     |
                |180       |name180   |180.0     |
                |198       |name198   |198.0     |
                Total records: 2""",res);
        res = clientTester.send("SELECT * FROM test WHERE id = 180 OR name = 'name198'");
        assertEquals("""

                |id        |name      |float     |
                |180       |name180   |180.0     |
                |198       |name198   |198.0     |
                Total records: 2""",res);
        res = clientTester.send("SELECT * FROM test where id = 99 OR id = 80");
        assertEquals("""

                |id        |name      |float     |
                |80        |name80    |80.0      |
                |99        |name99    |99.0      |
                Total records: 2""",res);
        res = clientTester.send("SELECT * FROM test where (id = 99 AND name = 'name99') OR (id = 80 AND name = 'name80')");
        assertEquals("""

                |id        |name      |float     |
                |80        |name80    |80.0      |
                |99        |name99    |99.0      |
                Total records: 2""",res);
        res = clientTester.send("SELECT * FROM test where id = 97 OR id = 70");
        assertEquals("""

                |id        |name      |float     |
                |70        |name70    |70.0      |
                |97        |name97    |97.0      |
                Total records: 2""",res);
        res = clientTester.send("SELECT * FROM test where id = 97 AND name = 'name97' OR float = 70.0");
        assertEquals("""

                |id        |name      |float     |
                |70        |name70    |70.0      |
                |97        |name97    |97.0      |
                Total records: 2""",res);
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
        res = clientTester.send("SELECT * FROM test WHERE id = id");
        assertEquals("""

                |id        |name      |float     |
                |0         |name0     |0.0       |
                |1         |name1     |1.0       |
                |2         |name2     |2.0       |
                Total records: 3""",res);
        clientTester.close();
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
        res = clientTester.send("SELECT * FROM test WHERE 1 = 1");
        assertEquals("""

                |id        |name      |float     |
                |0         |name0     |0.0       |
                |1         |name1     |1.0       |
                |2         |name2     |2.0       |
                Total records: 3""",res);
        clientTester.close();
    }

    @Test
    public void wiredExpressionTest3() throws IOException, InterruptedException {
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

        res = clientTester.send("SELECT id,name,age FROM test WHERE id = 1");
        assertEquals("\n" +
                "Field age not found in Expression Evaluation Cache, please make sure calculate the expression in order",res);
        res = clientTester.send("SELECT id,name,age FROM test");
        assertEquals("\n" +
                "Field age not found in Expression Evaluation Cache, please make sure calculate the expression in order",res);

        clientTester.close();
    }

    @Test
    public void testExpressionInSelect() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |age       |name      |float     |
                |0         |1         |name0     |0.0       |
                |1         |2         |name1     |1.0       |
                |2         |3         |name2     |2.0       |
                Total records: 3""",res);
        res = clientTester.send("SELECT id - age AS delta FROM test WHERE id = 1");
        assertEquals("""

                |delta     |
                |-1        |
                Total records: 1""",res);
        res = clientTester.send("SELECT id + age AS total FROM test WHERE id = 1");
        assertEquals("""

                |total     |
                |3         |
                Total records: 1""",res);
        res = clientTester.send("SELECT id + age AS total FROM test");
        assertEquals("""

                |total     |
                |1         |
                |3         |
                |5         |
                Total records: 3""",res);
        clientTester.close();
    }

    @Test
    public void expressionWithAliasNoWhere() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");

        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test");
        assertEquals("""

                |total     |delta1    |
                |1         |2         |
                |3         |5         |
                |5         |8         |
                Total records: 3""",res);
        clientTester.send("SELECT total + age AS delta1,id + age AS total FROM test");

        clientTester.close();
    }

    @Test
    public void expressionWithAlias() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test WHERE id = 1");
        assertEquals("""

                |total     |delta1    |
                |3         |5         |
                Total records: 1""",res);
    }

    @Test
    public void expressionArithmeticalError() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT id + name AS total,total + age AS delta1 FROM test WHERE id = 1");
        assertEquals("\n" +
                "Integer type can not be plused with a non-numeric type",res);
        res = clientTester.send("SELECT id - float AS total FROM test WHERE id = 1");
        assertEquals("""

                |total     |
                |0.0       |
                Total records: 1""",res);
        res = clientTester.send("SELECT id + float AS total FROM test WHERE id = 1");
        assertEquals("""

                |total     |
                |2.0       |
                Total records: 1""",res);
        clientTester.close();
    }

    @Test
    public void multiTableSelect() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        clientTester.send("CREATE TABLE test2 (id2 INT,age2 int, name2 VARCHAR(20),float2 FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test2 VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test2 insert 1 records",res);
        }
        res = clientTester.send("SELECT id,id2 FROM test,test2 WHERE id = id2");
        assertEquals("""

                |id        |id2       |
                |0         |0         |
                |1         |1         |
                |2         |2         |
                Total records: 3""",res);
        res = clientTester.send("SELECT id,id2 FROM test,test2 WHERE id = id2 AND id = 1");
        assertEquals("""

                |id        |id2       |
                |1         |1         |
                Total records: 1""",res);
        clientTester.close();
    }

    @Test
    public void multiTableSelectDuplicateColumnName() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test");
        assertEquals("""

                |id        |age       |name      |float     |
                |0         |1         |name0     |0.0       |
                |1         |2         |name1     |1.0       |
                |2         |3         |name2     |2.0       |
                Total records: 3""",res);
        clientTester.send("CREATE TABLE test2 (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 3; i++) {
            res = clientTester.send("INSERT INTO test2 VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test2 insert 1 records",res);
        }
        res = clientTester.send("SELECT * FROM test2");
        assertEquals("""

                |id        |age       |name      |float     |
                |0         |1         |name0     |0.0       |
                |1         |2         |name1     |1.0       |
                |2         |3         |name2     |2.0       |
                Total records: 3""",res);
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

    @Test
    public void testIndex() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 100; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }
        res = clientTester.send("CREATE INDEX test_index ON test(id)");
        assertEquals("\n" +
                "Create index test_index on test.id success",res);
        res = clientTester.send("CREATE INDEX test_index_name ON test(name)");
        assertEquals("\n" +
                "Create index test_index_name on test.name success",res);
        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test WHERE id = 1");
        assertEquals("""

                |total     |delta1    |
                |3         |5         |
                Total records: 1""",res);

        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test WHERE id = 1 OR name = 'name2'");
        assertEquals("""

                |total     |delta1    |
                |3         |5         |
                |5         |8         |
                Total records: 2""",res);

        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test WHERE id = 1 AND name = 'name1'");
        assertEquals("""

                |total     |delta1    |
                |3         |5         |
                Total records: 1""",res);

        res = clientTester.send("SELECT id + age AS total,total + age AS delta1 FROM test WHERE (id = 1 AND name = 'name1') OR id = 2");
        assertEquals("""

                |total     |delta1    |
                |3         |5         |
                |5         |8         |
                Total records: 2""",res);
        clientTester.close();
    }

    @Test
    public void testHashJoin() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        clientTester.send("CREATE TABLE test (id INT,age int, name VARCHAR(20),float FLOAT)");
        for (int i = 0; i < 100; i++) {
            res = clientTester.send("INSERT INTO test VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test insert 1 records",res);
        }

        clientTester.send("CREATE TABLE test1 (id1 INT,age1 int, name1 VARCHAR(20),float1 FLOAT)");
        for (int i = 0; i < 100; i+= 3) {
            res = clientTester.send("INSERT INTO test1 VALUES (" + i + "," + (i+1) + ", 'name" + i + "'," + i + ".0)");
            assertEquals("\n" +
                    "Table test1 insert 1 records",res);
        }

        res = clientTester.send("SELECT id + id1 AS tid, id + float1 AS ff, name,id FROM test,test1 WHERE id = id1");
        assertTrue(res.contains("Total records: 34"));
        clientTester.close();

    }
}
