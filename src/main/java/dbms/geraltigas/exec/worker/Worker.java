package dbms.geraltigas.exec.worker;

import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import dbms.geraltigas.exec.worker.handler.Handler;
import dbms.geraltigas.exec.worker.handler.HandlerFactory;
import dbms.geraltigas.exec.worker.handler.impl.ShowHandler;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Component
public class Worker implements Runnable {
    @Override
    public void run() {
        System.out.println("["+threadId+"] "+"Worker started");
        InputStream inputStream;
        OutputStream outputStream;
        try {
            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(inputStream));
        String line;
        try {
            outputStream.write("OK".getBytes());
            outputStream.write("[Server] Welcome to Geraltigas DBMS!".getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            while (true) {
                socket.sendUrgentData(0xFF);

                while ((line = reader.readLine()) != null) {
                    System.out.println("["+threadId+"] "+"Received: " + line);
                    String res = doWork(line)+"\n";
                    outputStream.write(res.getBytes());
                }
            }
        } catch (IOException e) {
            System.out.println("["+threadId+"] "+"Client disconnected");
            try {
                socket.close();
                System.out.println("["+threadId+"] "+"Socket closed");
                return;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        catch ( JSQLParserException | HandleException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } catch (DataTypeException | DropTypeException | ExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public enum State { IDLE, CONNECTED, AUTHENTICATED, }

    @Autowired
    HandlerFactory handlerFactory;
    @Autowired
    ExecutorService executorService;

    private State state = State.IDLE;
    private Socket socket;

    private long threadId;

    private Handler handler;

    // public
    public Worker(Socket socket) {
        this.socket = socket;
        this.state = State.CONNECTED;
    }

    public Worker() {
    }

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
        if (input.contains("AUTH")) {
            String[] tokens = input.split(" ");
            if (tokens.length != 3) {
                return "Invalid AUTH command";
            }
            String username = tokens[1];
            String password = tokens[2];
            if (username.equals("root") && password.equals("root")) {
                this.state = State.AUTHENTICATED;
                workerPrint("AUTH OK: " + username);
                return "AUTH OK: root";
            }if (username.contains("user") && username.equals(password)) {
                this.state = State.AUTHENTICATED;
                workerPrint("AUTH OK: " + username);
                return "AUTH OK: " + username;
            } else {
                return "AUTH FAIL";
            }
        }
        else {
            return "Please login first: AUTH username password";
        }
    }

    public String authenticatedProcess(String input) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException {

        String[] statements = input.split(";");

        List<String> res = new LinkedList<>();
        
        for (String statement : statements) {
            if (statement.trim().length() == 0) {
                continue;
            }else if ("BEGIN".equalsIgnoreCase(statement.trim())) {
                res.add(beginExec());
            } else if ("COMMIT".equalsIgnoreCase(statement.trim())) {
                res.add(commitExec());
            }else if ("ROLLBACK".equalsIgnoreCase(statement.trim())) {
                res.add(rollbackExec());
            }else if (statement.toUpperCase().contains("SHOW")) {
                res.add(showExec(statement));
            }
            else{
                try {
                    Statement stmt = CCJSqlParserUtil.parse(statement);
                    String temp = routeExec(stmt);
                    res.add(temp);
                } catch (JSQLParserException e) {
                    System.out.println("Sql Syntax Error: " + e.getMessage());
                    return "Sql Syntax Error";
                }
            }
        }

        return String.join(";\n", res);
    }

    private String showExec(String statement) throws HandleException, DataTypeException, DropTypeException, ExpressionException, ExecutionException, InterruptedException {
        ShowHandler handler = (ShowHandler) handlerFactory.getHandler(HandlerFactory.HandlerType.SHOW);
        handler.setThreadId(threadId);
        this.handler = handler;
        return waitForResult(handler.handleShow(statement));
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
                return "Unimplemented";
                // return updateExec((Update) statement);
            case "CreateIndex":
                return createIndexExec((CreateIndex) statement);
            default:
                return "Unsupported Statement";
        }
    }
    private String beginExec() throws HandleException, DataTypeException, DropTypeException, ExpressionException, ExecutionException, InterruptedException { //TODO: begin transaction
        workerPrint("Begin Transaction");
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.BEGIN);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(null));
    }
    private String commitExec() { //TODO: commit transaction
        return null;
    }
    private String rollbackExec() { //TODO: rollback transaction
        return null;
    }

    private String createIndexExec(CreateIndex statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Create Index
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.CREATE_INDEX);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(statement));
    }

    private String updateExec(Update statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Update
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.UPDATE);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(statement));
    }

    private String insertExec(Insert statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Insert
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.INSERT);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(statement));
    }

    private String dropExec(Drop statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Drop
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.DROP);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(statement));
    }

    private String createTableExec(CreateTable statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Create Table
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.CREATE_TABLE);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(statement));
    }

    private String deleteExec(Delete statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Delete
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.DELETE);
        handler.setThreadId(threadId);
        return waitForResult(handler.handle(statement));
    }

    private String selectExec(Select statement) throws HandleException, ExecutionException, InterruptedException, DataTypeException, DropTypeException, ExpressionException { //TODO: Select
        handler = handlerFactory.getHandler(HandlerFactory.HandlerType.SELECT);
        handler.setThreadId(threadId);
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
        String res = future.get();
        workerPrint("Result: " + res);
        return res;
    }

    private void workerPrint(String str) {
        System.out.println("["+threadId+"] "+str);
    }
}
