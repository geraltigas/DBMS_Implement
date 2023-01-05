package dbms.geraltigas.client.test.multi;

import dbms.geraltigas.client.ClientTester;
import dbms.geraltigas.client.test.MultiShower;
import dbms.geraltigas.utils.Printer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.swing.plaf.PanelUI;
import java.io.IOException;
import java.util.List;

@SpringBootTest
public class TransactionTest {
    @Test
    public void readLockAndWriteLockTest() throws IOException, InterruptedException {
        MultiShower multiShower = new MultiShower(List.of("A", "B"));
        multiShower.addScript("A", "clearAllData");
        // AUTH
        multiShower.addScript("A", "AUTH root root");
        multiShower.addScript("B", "AUTH user user");
        multiShower.addScript("A", "create table t1 (id int, name varchar(20))");
        multiShower.addScript("A", "insert into t1 values (1, 'a')");
        multiShower.addScript("A", "insert into t1 values (2, 'b')");
        multiShower.addScript("A", "insert into t1 values (3, 'c')");
        multiShower.addScript("A", "begin");
        multiShower.addScript("A", "select * from t1");
        multiShower.addScript("B", "select * from t1");
        //a insert
        multiShower.addScript("A", "insert into t1 values (4, 'd')");
        // b insert
        multiShower.addScript("B", "insert into t1 values (5, 'e')");
        multiShower.addScript("A","commit");
        // b show
        multiShower.addScript("B", "select * from t1");
        multiShower.addScript("A", "stop");
        multiShower.addScript("B", "stop");
        multiShower.begin();
    }

    @Test
    public void tablePageRollBackTest() throws IOException, InterruptedException {
        MultiShower multiShower = new MultiShower(List.of("A", "B"));
        multiShower.addScript("A", "clearAllData");
        // AUTH
        multiShower.addScript("A", "AUTH root root");
        multiShower.addScript("B", "AUTH user user");
        multiShower.addScript("A", "create table t1 (id int, name varchar(20))");
        multiShower.addScript("A", "insert into t1 values (1, 'a')");
        multiShower.addScript("A", "insert into t1 values (2, 'b')");
        multiShower.addScript("A", "insert into t1 values (3, 'c')");
        multiShower.addScript("A", "begin");
        multiShower.addScript("A", "select * from t1");
        multiShower.addScript("B", "select * from t1");
        //a insert
        multiShower.addScript("A", "insert into t1 values (4, 'd')");
        // b insert
        multiShower.addScript("B", "insert into t1 values (5, 'e')");
        multiShower.addScript("A","rollback");
        // b show
        multiShower.addScript("B", "select * from t1");
        multiShower.addScript("A", "stop");
        multiShower.addScript("B", "stop");
        multiShower.begin();
    }

    @Test
    public void fileCreateRollBackTest() throws IOException, InterruptedException {
        MultiShower multiShower = new MultiShower(List.of("A", "B"));
        multiShower.addScript("A", "clearAllData");
        // AUTH
        multiShower.addScript("A", "AUTH root root");
        multiShower.addScript("B", "AUTH user user");
        multiShower.addScript("A", "create table t1 (id int, name varchar(20))");
        multiShower.addScript("A", "insert into t1 values (1, 'a')");
        multiShower.addScript("A", "insert into t1 values (2, 'b')");
        multiShower.addScript("A", "insert into t1 values (3, 'c')");
        // b select
        multiShower.addScript("B", "select * from t1");
        multiShower.addScript("A", "begin");
        multiShower.addScript("A", "select * from t1");
        // create a table
        multiShower.addScript("A", "create table t2 (id int, name varchar(20))");
        // A insert
        multiShower.addScript("A", "insert into t1 values (4, 'd')");
        multiShower.addScript("A", "create index t1_id on t1(id)");
        multiShower.addScript("A","rollback");
        // B show
        multiShower.addScript("B", "select * from t1");
        multiShower.addScript("B", "select * from t1 where id = 4");
        // show tables
        multiShower.addScript("B", "show tables");
        multiShower.addScript("A", "stop");
        multiShower.addScript("B", "stop");
        multiShower.begin();
    }

    @Test
    public void indexPageRollBackTest() throws IOException, InterruptedException {
        MultiShower multiShower = new MultiShower(List.of("A", "B"));
        multiShower.addScript("A", "clearAllData");
        // AUTH
        multiShower.addScript("A", "AUTH root root");
        multiShower.addScript("B", "AUTH user user");
        multiShower.addScript("A", "create table t1 (id int, name varchar(20))");
        multiShower.addScript("A", "insert into t1 values (1, 'a')");
        multiShower.addScript("A", "insert into t1 values (2, 'b')");
        multiShower.addScript("A", "insert into t1 values (3, 'c')");
        // create index
        multiShower.addScript("A", "create index t1_id on t1(id)");
        multiShower.addScript("A", "begin");
        multiShower.addScript("A", "select * from t1");
        multiShower.addScript("B", "select * from t1");
        //a insert
        multiShower.addScript("A", "insert into t1 values (4, 'd')");
        // b insert
        multiShower.addScript("B", "insert into t1 values (5, 'e')");
        multiShower.addScript("A","rollback");
        // b show
        multiShower.addScript("B", "select * from t1");
        multiShower.addScript("A", "stop");
        multiShower.addScript("B", "stop");
        multiShower.begin();
    }

    @Test
    public void tableHeaderRollback() throws IOException, InterruptedException {
        ClientTester.begin();
        ClientTester clientTester = new ClientTester("root");
        clientTester.clearAllData();
        String res;
        clientTester.send("AUTH root root");
        res = clientTester.send("create table t1 (id int, name varchar(20))");
        // INDEX
        res = clientTester.send("begin");
        res = clientTester.send("create index t1_id on t1(id)");
        for (int i = 0; i < 200; i++) {
            clientTester.send("insert into t1 values (" + i + ", 'a')");
        }
        clientTester.send("rollback");
        // INSERT
        res = clientTester.send("insert into t1 values (1, 'a')");
        res = clientTester.send("select * from t1 WHERE id = 1");
        clientTester.close();
    }

    @Test
    public void deadLockDetect() throws IOException, InterruptedException {
        MultiShower multiShower = new MultiShower(List.of("A", "B"));
        multiShower.addScript("A", "clearAllData");
        // AUTH
        multiShower.addScript("A", "AUTH root root");
        multiShower.addScript("B", "AUTH user user");
        multiShower.addScript("A", "create table t1 (id int, name varchar(20))");
        multiShower.addScript("A", "insert into t1 values (1, 'a')");
        multiShower.addScript("A", "insert into t1 values (2, 'b')");
        multiShower.addScript("A", "insert into t1 values (3, 'c')");
        // create index
        multiShower.addScript("A", "begin");
        // b begin
        multiShower.addScript("B", "begin");
        multiShower.addScript("A", "select * from t1");
        multiShower.addScript("B", "select * from t1");
        //a insert
        multiShower.addScript("A", "insert into t1 values (4, 'd')");
        // b insert
        multiShower.addScript("B", "insert into t1 values (5, 'e')");
        multiShower.addScript("B", "wait");
        multiShower.addScript("A", "wait");
//        multiShower.addScript("A", "stop");
//        multiShower.addScript("B", "stop");
        multiShower.begin();
        Thread.sleep(120000);
    }
}
