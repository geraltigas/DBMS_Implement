package dbms.geraltigas.client.test;

import dbms.geraltigas.client.ClientTester;
import dbms.geraltigas.client.MultiTester;
import dbms.geraltigas.utils.Printer;

import java.io.IOException;
import java.io.PipedInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;

class ClientTestWrap implements Comparable<ClientTestWrap>,Runnable {
    ClientTester clientTester;
    Deque<String> scripts = new ConcurrentLinkedDeque<>();
    public ClientTestWrap(ClientTester clientTester) {
        this.clientTester = clientTester;
    }

    public void addScript(String script) {
        scripts.add(script);
    }

    boolean isWaiting = false;

    @Override
    public int compareTo(ClientTestWrap o) {
        return clientTester.name.compareTo(o.clientTester.name);
    }

    @Override
    public void run() {
        while(true) {
            String script = scripts.poll();
            if (script == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            try {
                switch (script) {
                    case "stop":
                        if (isWaiting) {
                            clientTester.close();
                        }
                        return;
                    case "clearAllData":
                        clientTester.clearAllData();
                        break;
                    case "wait":
                        clientTester.close();
                        break;
                    default:
                        clientTester.send(script);
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

class Script {
    public String clientName;
    public String script;
    public Script(String clientName,String script) {
        this.clientName = clientName;
        this.script = script;
    }
}

public class MultiShower {

    Map<String,ClientTestWrap> clientTesterMap = new TreeMap<>();

    List<Script> scripts = new ArrayList<>();

    ExecutorService executorService = new java.util.concurrent.ForkJoinPool(10);

    public MultiShower(List<String> names) throws IOException, InterruptedException {
        for (String name: names) {
            clientTesterMap.put(name,new ClientTestWrap(new ClientTester(name)));
        }
    }

    public void addScript(String clientName,String script) {
        scripts.add(new Script(clientName,script));
    }

    public void begin() throws InterruptedException {
        for (ClientTestWrap clientTesterWrap : clientTesterMap.values()) {
            executorService.submit(clientTesterWrap);
        }
        for (Script script : scripts) {
            Thread.sleep(1000);
            Printer.print("Begin next script","Info");
            clientTesterMap.get(script.clientName).addScript(script.script);
            while (clientTesterMap.get(script.clientName).scripts.size() > 0) {
                Thread.sleep(1000);
            }
        }
    }

}
