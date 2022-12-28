package dbms.geraltigas.net.listener;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.exec.worker.Worker;
import jakarta.annotation.PostConstruct;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

@Component
public class Listener {
    private static final int PORT = 12345;
    private static final String HOST = "localhost";

    @Autowired
    ExecutorService executorService;

    @PostConstruct
    public void init() throws IOException {
        System.out.println("[Server] Server listening on port " + PORT);
        executorService.submit(() -> {
            try {
                listen();
            } catch (IOException e) {
                System.out.println("[Server] Server stopped listening");
                throw new RuntimeException(e);
            }
        });
        System.out.println("[Server] Server started");
    }

    private void listen() throws IOException {
        System.out.println("[Server] Server into listening");
        ServerSocket serverSocket = new ServerSocket(PORT);
        while (true) {
            Socket socket = serverSocket.accept();
            Worker worker = new Worker(socket);
            ApplicationContextUtils.autowire(worker);
            Thread thread = new Thread(worker);
            long pid = thread.getId();
            worker.setThreadId(pid);
            thread.start();
            System.out.println("["+pid+"] "+"Client connected");
        }
    }
}
