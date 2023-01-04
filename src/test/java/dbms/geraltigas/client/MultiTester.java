package dbms.geraltigas.client;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;

public class MultiTester {

    public boolean isClosed() {
        for (Map.Entry<String,ClientTesterWrap> entry: clientTesterMap.entrySet()) {
            if (!entry.getValue().clientTester.isClosed) return true;
        }
        return false;
    }

    public enum ScriptType {
        SQL,
        SLEEP,
        STOP,
        CLEAR_ALL
    }

    public class Script {
        public String clientName;
        public String script;
        public ScriptType type;
        public Script(String clientName, ScriptType type,String script) {
            this.clientName = clientName;
            this.script = script;
            this.type = type;
        }
    }

    List<Script> scripts = new ArrayList<>();

    OpMode opMode = OpMode.AUTO;

    Map<String,ClientTesterWrap> clientTesterMap = new TreeMap<>();

    public ExecutorService executorService = new java.util.concurrent.ForkJoinPool(10);

    class ClientTesterWrap implements Runnable {
        ClientTester clientTester;

        Deque<Script> orders = new ConcurrentLinkedDeque<>();

        public ClientTesterWrap(ClientTester clientTester) {
            this.clientTester = clientTester;
        }

        public void addOrder(Script script) {
            orders.add(script);
        }

        Map<Long,String> result = new TreeMap<>();

        @Override
        public void run() {
            String res;
            while (true) {
                if (clientTester.isClosed) break;
                if (orders.size() > 0) {
                    Script script = orders.poll();
                    try {
                        res = clientTester.send(script.script);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    result.put(System.currentTimeMillis(),res);
                }
                Thread.yield();
            }
        }

        public Map<Long,String> getResult() {
            return result;
        }
    }

    public enum OpMode {
        AUTO,
        MANUAL
    }

    public MultiTester(List<String> clientNames,OpMode opMode) throws IOException, InterruptedException {
        this.opMode = opMode;
        for (String clientName : clientNames) {
            clientTesterMap.put(clientName,new ClientTesterWrap(new ClientTester(clientName)));
            executorService.submit(clientTesterMap.get(clientName));
        }
    }

    public void addScript(String clientName,ScriptType type,String script) {
        scripts.add(new Script(clientName,type,script));
    }

    public Map<Long, String> run() throws IOException, InterruptedException {
        for (Script script : scripts) {
            switch (script.type) {
                case SQL -> clientTesterMap.get(script.clientName).addOrder(script);
                case SLEEP -> {
                    try {
                        Thread.sleep(Long.parseLong(script.script));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                case STOP -> {
                    clientTesterMap.get(script.clientName).clientTester.close();
                }
                case CLEAR_ALL -> clientTesterMap.get(script.clientName).clientTester.clearAllData();
            }
        }
        // wait for all client tester to finish
        System.out.println("Waiting for all client tester to finish");
        Thread.sleep(5000);
        Map<Long,String> timeStampResultSet = new TreeMap<>();
        for (Map.Entry<String,ClientTesterWrap> entry : clientTesterMap.entrySet()) {
            for (Map.Entry<Long,String> entry1 : entry.getValue().getResult().entrySet()) {
                timeStampResultSet.put(entry1.getKey(),"["+entry.getKey()+"] " + " : " + entry1.getValue());
                // close
                entry.getValue().clientTester.close();
            }
        }
        return timeStampResultSet;
    }

}
