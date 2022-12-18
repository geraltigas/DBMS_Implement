package dbms.geraltigas.exec.worker;

import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import dbms.geraltigas.exec.worker.handler.HandlerFactory;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Component
public class Worker {
    public enum State { IDLE, CONNECTED, AUTHENTICATED, }

    @Autowired
    HandlerFactory handlerFactory;
    @Autowired
    ExecutorService executorService;

    private State state = State.IDLE;
    private Socket socket;

    private Boolean inTransaction = false;

    private Handler handler;

    // public
    public Worker() {}

    public String doWork(String input) throws JSQLParserException, HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException {
        switch (state) {
            case IDLE:
                return idleProcess(input);
            case CONNECTED:
                return connectedProcess(input);
            case AUTHENTICATED:
                return authenticatedProcess(input);
            default:
                return "UNKNOWN";
        }
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
        this.state = State.CONNECTED;
    }

    public void printState() {
        System.out.println("State: " + state);
    }

    public void setState(State state) {
        this.state = state;
    }

    // private

    private String idleProcess(String input) {
        this.state = State.CONNECTED;
        return "CONN" + input;
    }

    private String connectedProcess(String input) {
        this.state = State.AUTHENTICATED;
        return "AUTH" + input;
    }

    public String authenticatedProcess(String input) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException {

        String[] statements = input.split(";");

        List<String> res = new LinkedList<>();
        
        for (String statement : statements) {
            if (statement.trim().length() == 0) {
                continue;
            }else if ("BEGIN".equalsIgnoreCase(statement.trim())) {
                this.inTransaction = true;
                beginExec();
            } else if ("COMMIT".equalsIgnoreCase(statement.trim())) {
                commitExec();
                this.inTransaction = false;
            }else if ("ROLLBACK".equalsIgnoreCase(statement.trim())) {
                this.inTransaction = false;
                rollbackExec();
            }else{
                try {
                    Statement stmt = CCJSqlParserUtil.parse(statement);
                    String temp = routeExec(stmt);
                    System.out.println(temp);
                    res.add(temp);
                } catch (JSQLParserException e) {
                    System.out.println("Sql Syntax Error: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        return String.join(";\n", res);
    }

    private String routeExec(Statement statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException {
        switch (statement.getClass().getSimpleName()){
            case "Select":
                return selectExec((Select) statement);
            case "Delete":
                return deleteExec((Delete) statement);
            case "CreateTable":
                return createTableExec((CreateTable) statement);
            case "Drop":
                return dropExec((Drop) statement);
            case "Insert":
                return insertExec((Insert) statement);
            case "Update":
                return "Unimplemented Update";
                // return updateExec((Update) statement);
            case "CreateIndex":
                return createIndexExec((CreateIndex) statement);
            default:
                return "Unsupported Statement";
        }
    }
    private void beginExec() { //TODO: begin transaction

    }
    private void commitExec() { //TODO: commit transaction

    }
    private void rollbackExec() { //TODO: rollback transaction

    }

    private String createIndexExec(CreateIndex statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Create Index
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.CREATE_INDEX);
        return waitForResult(handler.handle(statement));
    }

    private String updateExec(Update statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Update
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.UPDATE);
        return waitForResult(handler.handle(statement));
    }

    private String insertExec(Insert statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Insert
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.INSERT);
        return waitForResult(handler.handle(statement));
    }

    private String dropExec(Drop statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Drop
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.DROP);
        return waitForResult(handler.handle(statement));
    }

    private String createTableExec(CreateTable statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Create Table
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.CREATE_TABLE);
        return waitForResult(handler.handle(statement));
    }

    private String deleteExec(Delete statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Delete
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.DELETE);
        return waitForResult(handler.handle(statement));
    }

    private String selectExec(Select statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Select
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.SELECT);
        return waitForResult(handler.handle(statement));
    }

    private String waitForResult(int handle) throws InterruptedException, ExecutionException {
        int hash = handle;
        Future<String> future = executorService.submit(() -> {
            while (true) {
                String res = handler.getResault(hash);
                if (res != null) {
                    return res;
                }
                Thread.yield();
            }
        });
        return future.get();
    }
}
