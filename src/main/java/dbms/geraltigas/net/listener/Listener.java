package dbms.geraltigas.net.listener;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.exec.worker.Worker;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

@Component
public class Listener {
    private static final int PORT = 12345;
    private static final String HOST = "localhost";


    @PostConstruct
    public void init() throws IOException {
        System.out.println("[Server] Server listening on port " + PORT);
        listen();
    }

    private void listen() throws IOException {
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
