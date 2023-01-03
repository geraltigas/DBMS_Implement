package dbms.geraltigas.client.test.single;

import dbms.geraltigas.client.ClientTester;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class CreateTableTest {
    @Test
    public void test() {
        AtomicBoolean thread0Ok = new AtomicBoolean(false);
        Thread thread0 = new Thread(() -> {
            try {
                String res = "";
                ClientTester.begin();
                ClientTester clientTester = new ClientTester("root");
                clientTester.clearAllData();
                clientTester.send("AUTH root root");
                // wrong sql
                res = clientTester.send("CREATE TABLE test (id INT, name VARCHAR(20)");
                System.out.println(res);
                assertEquals("\nSql Syntax Error",res);
                // no column
                res = clientTester.send("CREATE TABLE test1 ()");
                assertEquals("\nNo column definitions",res);
                // single column, one type
                res = clientTester.send("CREATE TABLE test2 (id INT)");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                res = clientTester.send("CREATE TABLE test3 (name VARCHAR(20))");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                res = clientTester.send("CREATE TABLE test4 (id FLOAT)");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                // same table name
                res = clientTester.send("CREATE TABLE test2 (id int, name varchar(20))");
                assertEquals("\n" +
                        "Table file already exists;\n" +
                        "Create table failed",res);
                // multiple columns, one type same column name
                res = clientTester.send("CREATE TABLE test5 (id INT, id INT)");
                assertEquals("\n" +
                        "Duplicate column name",res);
                res = clientTester.send("CREATE TABLE test6 (id INT, id INT,id INT)");
                assertEquals("\n" +
                        "Duplicate column name",res);
                // multiple columns, one type different column name
                res = clientTester.send("CREATE TABLE test7 (id INT, id2 int)");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                res = clientTester.send("CREATE TABLE test8 (id INT, id2 int)");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                // multiple columns, multiple type
                res = clientTester.send("CREATE TABLE test9 (id INT, name VARCHAR(20))");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                res = clientTester.send("CREATE TABLE test10 (id INT, name VARCHAR(20), age INT)");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                res = clientTester.send("CREATE TABLE test11 (id INT, name VARCHAR(20), age INT, float FLOAT)");
                assertEquals("\n" +
                        "Create table file;\n" +
                        "Write table metadata;\n" +
                        "Write table header",res);
                // not support type
                res = clientTester.send("CREATE TABLE test12 (id INT, name VARCHAR(20), age INT, float FLOAT, date DATE)");
                assertEquals("\n" +
                        "DataType not support",res);
                res = clientTester.send("CREATE TABLE test13 (id INT, name VARCHAR(20), age INT, float FLOAT)");
                assertEquals("\n" +
                                "Create table file;\n" +
                                "Write table metadata;\n" +
                                "Write table header",res);
                assertEquals(1,2);
                clientTester.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            thread0Ok.set(true);
        });
        thread0.start();
        // wait the thread to finish
        try {
            thread0.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(thread0Ok.get());
    }
}
