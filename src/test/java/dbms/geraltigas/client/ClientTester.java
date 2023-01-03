package dbms.geraltigas.client;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.transaction.LockManager;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.*;
import java.net.Socket;
import java.nio.file.Path;

// TODO: deadlock check
// TODO: empty hole remove
// TODO: transaction test
// TODO: pageBuffer remove page into list for recycle

public class ClientTester {
    String name;
    static String host = "localhost";
    static String port = "12345";
    private Socket socket;
    private OutputStream outputStream;
    private BufferedReader bufferedReader;
    @Autowired
    ExecuteEngine executeEngine;
    @Autowired
    LockManager lockManager;
    public ClientTester(String name) throws IOException, InterruptedException {
        ApplicationContextUtils.autowire(this);
        Socket socket = new Socket(host, Integer.parseInt(port));
        this.socket = socket;
        this.name = name;
        this.outputStream = socket.getOutputStream();
        this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String message = "";
        Thread.sleep(1000);
        if (bufferedReader.ready()) {
            message = bufferedReader.readLine();
        }
        if (message.contains("OK")) {
            print("Receive:\n"+"Connected to server");
            message = message.replace("OK", "");
        }
        if (message == "") {
            message = bufferedReader.readLine();
        }
        print("Receive:\n"+message);
    }

    private void print(String message) {
        String str = "\n{\n";
        str += message;
        str += "\n}\n";
        System.out.println("\033[32;4m"+"["+name+"] "+str+"\033[0m");
    }

    public String send(String message) throws IOException, InterruptedException {
        print("Send:\n"+message);
        outputStream.write((message+"\n").getBytes());
        outputStream.flush();
        String line = "";
        while (!bufferedReader.ready()) {
        }
        while (bufferedReader.ready()) {
            line = line + "\n" + bufferedReader.readLine();
        }
        print("Receive:"+line);
        return line;
    }

    public void close() throws IOException, InterruptedException {

        print("Waiting for data persist");
        Thread.sleep(10000);
        socket.close();
        print("Client closed");
    }

    public static void begin() throws IOException, InterruptedException {
        Thread.sleep(1000);
    }

    public void clearAllData() throws InterruptedException {
        Thread.sleep(1000);
        Path path = Path.of(executeEngine.getDateDir());
        File file = path.toFile();
        // delete this directory and all its subdirectories
        for (File f : file.listFiles()) {

            // delete all files in this directory
            for (File f1 : f.listFiles()) {
                if (f1.isFile()) {
                    f1.delete();
                }else {
                    for (File f2 : f1.listFiles()) {
                        f2.delete();
                    }
                    f1.delete();
                }
            }
        }
        print("All data cleared");
    }
}
