package dbms.geraltigas.utils;

public class Printer {
    public static void print(String s,long threadId) {
        System.out.println("["+threadId+"] "+s);
    }
}
