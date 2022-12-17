package dbms.geraltigas.utils;

import dbms.geraltigas.format.tables.TableDefine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataDumpTest {

    @Test
    void intToBytes() {
        int value = 123456789;
        byte[] bytes = DataDump.IntToBytes(value);
        int result = DataDump.BytesToInt(bytes, 0);
        System.out.println(bytes.length);
        assertEquals(value, result);
    }

    @Test
    void floatToBytes() {
        float value = 123456789.123456789f;
        byte[] bytes = DataDump.FloatToBytes(value);
        float result = DataDump.BytesToFloat(bytes, 0);
        System.out.println(bytes.length);
        assertEquals(value, result);
    }

    @Test
    void stringToBytes() {
        String value = "123456789";
        byte[] bytes = DataDump.StringToBytes(value);
        String result = DataDump.BytesToString(bytes, 0);
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
        byte[] bytes = DataDump.DumpWithValid(typs, datas);
        List<Object> result = DataDump.Load(typs, bytes, 0);
        assertEquals(datas, result);
        assertEquals(datas.get(0),result.get(0));
        assertEquals(datas.get(1),result.get(1));
        assertEquals(datas.get(2),result.get(2));
    }
}