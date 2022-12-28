package dbms.geraltigas.utils;

import dbms.geraltigas.format.tables.TableDefine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.*;

class DataDumpTest {

    @Test
    void intToBytes() {
        int value = 123456789;
        byte[] bytes = DataDump.intToBytes(value);
        int result = DataDump.bytesToInt(bytes, 0);
        System.out.println(bytes.length);
        assertEquals(value, result);
    }

    @Test
    void floatToBytes() {
        float value = 123456789.123456789f;
        byte[] bytes = DataDump.floatToBytes(value);
        float result = DataDump.bytesToFloat(bytes, 0);
        System.out.println(bytes.length);
        assertEquals(value, result);
    }

    @Test
    void stringToBytes() {
        String value = "123456789";
        byte[] bytes = DataDump.stringToBytes(value);
        String result = DataDump.bytesToString(bytes, 0);
        assertEquals(value, result);
    }

    @Test
    void dump() {
        List<TableDefine.Type> typs = new ArrayList<>();
        typs.add(TableDefine.Type.INTEGER);
        typs.add(TableDefine.Type.FLOAT);
        typs.add(TableDefine.Type.VARCHAR);
        typs.add(TableDefine.Type.INTEGER);
        List<Object> datas = new ArrayList<>();
        datas.add(123456789);
        datas.add(123456789.123456789f);
        datas.add("123456789");
        datas.add(123123);
        byte[] bytes = DataDump.dumpWithValid(typs, datas);
        List<Object> result = DataDump.load(typs, bytes, 0);
        assertEquals(datas, result);
        assertEquals(datas.get(0),result.get(0));
        assertEquals(datas.get(1),result.get(1));
        assertEquals(datas.get(2),result.get(2));
    }

    @Test
    void run() throws InterruptedException {
        AtomicReference<Integer> value = new AtomicReference<>(123456789);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Thread thread = new Thread(() -> {
            ReentrantReadWriteLock.ReadLock readWriteLock = lock.readLock();
            readWriteLock.lock();
            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("thread1: "+i+" "+value);
            }
            ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
            System.out.println("thread1 try to upgrade lock");

            writeLock.lock();
            value.set(987654321);
            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("thread1: "+i+" "+value);
            }
            readWriteLock.unlock();
        });
        Thread thread1 = new Thread(() -> {
            ReentrantReadWriteLock.ReadLock readWriteLock = lock.readLock();
            readWriteLock.lock();
            for (int i = 0; i < 20; i++) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("thread2: "+i+" "+value);
            }
            readWriteLock.unlock();
        });
        thread.start();
        thread1.start();
        while (true){}
    }
}