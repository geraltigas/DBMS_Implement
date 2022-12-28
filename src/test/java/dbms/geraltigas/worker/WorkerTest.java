package dbms.geraltigas.worker;

import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.Worker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.ExecutionException;

@SpringBootTest
class WorkerTest {

    @BeforeEach
    void setUp() {

    }

    @Autowired
    Worker worker;


    @Test
    void doWork() {

    }

    @Test
    void authenticatedProcess() {
        try {
            worker.authenticatedProcess("CREATE TABLE test2 (id2 INT, age2 INT, age3 INT, age4 INT);");
//            worker.authenticatedProcess("INSERT INTO test1 VALUES (1, 2, 3, 4);");
//            worker.authenticatedProcess("INSERT INTO test1 VALUES (2, 3, 4, 5);");
//            worker.authenticatedProcess("INSERT INTO test1 VALUES (3, 4, 5, 6);");
//            worker.authenticatedProcess("SELECT * FROM test1;");
//            worker.authenticatedProcess("DELETE FROM test1 WHERE id = 1;");
            while (true) {}
//            worker.AuthenticatedProcess("DROP TABLE test1;");
            // SELECT age1 - age2 AS d, id FROM student WHERE id = age OR (age1 = 1 AND age2 = 2); //  SELECT * FROM test1;
            // CREATE TABLE test1 (id INT, age INT, age1 INT, age2 INT);
            // INSERT INTO test1 VALUES (1, 2, 3, 4);
            // INSERT INTO test1 VALUES (2, 3, 4, 5);
            // DROP TABLE test1;
//            System.out.println(str);
        } catch (HandleException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (DataTypeException e) {
            throw new RuntimeException(e);
        } catch (DropTypeException e) {
            throw new RuntimeException(e);
        } catch (ExpressionException e) {
            throw new RuntimeException(e);
        }
    }

}