package dbms.geraltigas.dataccess.execplan.impl;

import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.format.tables.TableDefine;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InsertExecTest {

    @Test
    void calculateLength() throws DataTypeException {
        List<TableDefine.Type> list = new LinkedList<>();
        list.add(TableDefine.Type.INTEGER);
        list.add(TableDefine.Type.VARCHAR);
        List<List<String>> attrs = new LinkedList<>();
        List<String> attr = new LinkedList<>();
        attrs.add(null);
        attr.add("10");
        attrs.add(attr);

        assertEquals(15, InsertExec.CalculateLength(list, attrs));
    }

    @Test
    void getOrderArray() {
        List<String> definedColNames = new LinkedList<>();
        definedColNames.add("a");
        definedColNames.add("b");
        definedColNames.add("c");
        String[] colNames = new String[]{"b", "c"};
        byte[] map = InsertExec.GetOrderArray(definedColNames, colNames);
        System.out.println((byte) -1);
        assertEquals((byte)-1, map[0]);
        assertEquals(0, map[1]);
        assertEquals(1,map[2]);
    }
}