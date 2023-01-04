package dbms.geraltigas.utils;

import org.springframework.boot.web.embedded.tomcat.TomcatReactiveWebServerFactory;

import java.util.Map;

public class Printer {
    private static boolean isDebug = false;

    public static void print(String s, long threadId) {
        if (threadId == -1) {
            System.out.println("[Scheduled Task] "+s);
        } else {
            System.out.println("["+threadId+"] "+s);
        }
    }

    public static void print(String s,String name) {
        System.out.println("\033[0;31m["+name+"] "+s+"\033[0m");
    }

    public static void printByTimeStamp(Map<Long, String> resultMap) {
        for (Map.Entry<Long, String> entry : resultMap.entrySet()) {
            System.out.println("\033[33;4m"+"["+entry.getKey()+"] \n"+entry.getValue()+"\033[0m");
        }
    }

    public static void DEBUG(String s) {
        if (isDebug) {
            System.out.println("\033[0;31m[DEBUG] "+s+"\033[0m");
        }
    }
}
