package dbms.geraltigas.utils;

import dbms.geraltigas.format.tables.TableDefine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class DataDump {

    public static byte[] IntToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static int BytesToInt(byte[] src, int offset) {
        int value;
        value = (int) ((src[offset + 3] & 0xFF)
                | ((src[offset + 2] & 0xFF) << 8)
                | ((src[offset + 1] & 0xFF) << 16)
                | ((src[offset] & 0xFF) << 24));
        return value;
    }

    public static byte[] FloatToBytes(float value) {
        int fbit = Float.floatToIntBits(value);
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (fbit >> (24 - i * 8));
        }
        return b;
    }

    public static float BytesToFloat(byte[] src, int offset) {
        int l;
        l = src[offset + 3];
        l &= 0xff;
        l |= ((long) src[offset + 2] << 8);
        l &= 0xffff;
        l |= ((long) src[offset + 1] << 16);
        l &= 0xffffff;
        l |= ((long) src[offset] << 24);
        return Float.intBitsToFloat(l);
    }

    public static byte[] StringToBytes(String value) {
        byte[] bytes = value.getBytes();
        byte[] temp = new byte[bytes.length + 4];
        System.arraycopy(IntToBytes(bytes.length), 0, temp, 0, 4);
        System.arraycopy(bytes, 0, temp, 4, bytes.length);
        return temp;
    }

    public static String BytesToString(byte[] src, int offset) {
        int len = BytesToInt(src, offset);
        return new String(src, 4+offset, len);
    }

    public static byte[] DumpWithValid(List<TableDefine.Type> typs, List<Object> datas) {
        int length = 0;
        for (int i = 0; i < typs.size(); i++) {
            switch (typs.get(i)) {
                case INTEGER, FLOAT -> length += 4;
                case VARCHAR -> length += ((String) datas.get(i)).length() + 4;
            }
        }
        byte[] result = new byte[length+1];
        result[0] = 1;
        int offset = 1; // skip the valid byte
        for (int i = 0; i < typs.size(); i++) {
            switch (typs.get(i)) {
                case INTEGER -> {
                    byte[] intBytes = IntToBytes((int) datas.get(i));
                    System.arraycopy(intBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case FLOAT -> {
                    byte[] floatBytes = FloatToBytes((float) datas.get(i));
                    System.arraycopy(floatBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case VARCHAR -> {
                    byte[] stringBytes = StringToBytes((String) datas.get(i));
                    System.arraycopy(stringBytes, 0, result, offset, stringBytes.length);
                    offset += stringBytes.length;
                }
            }
        }
        return result;
    }
    public static byte[] Dump(List<TableDefine.Type> typs, List<Object> datas) {
        int length = 0;
        for (int i = 0; i < typs.size(); i++) {
            switch (typs.get(i)) {
                case INTEGER, FLOAT -> length += 4;
                case VARCHAR -> length += ((String) datas.get(i)).length() + 4;
            }
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (int i = 0; i < typs.size(); i++) {
            switch (typs.get(i)) {
                case INTEGER -> {
                    byte[] intBytes = IntToBytes((int) datas.get(i));
                    System.arraycopy(intBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case FLOAT -> {
                    byte[] floatBytes = FloatToBytes((float) datas.get(i));
                    System.arraycopy(floatBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case VARCHAR -> {
                    byte[] stringBytes = StringToBytes((String) datas.get(i));
                    System.arraycopy(stringBytes, 0, result, offset, stringBytes.length);
                    offset += stringBytes.length;
                }
            }
        }
        return result;
    }

    public static byte[] DumpSrc(byte[] src,int per_size, List<TableDefine.Type> typs, List<List<Object>> records) {
        for (int i = 0; i < records.size(); i++) {
            byte[] temp = DumpWithValid(typs, records.get(i));
            System.arraycopy(temp, 0, src, i * per_size, temp.length);
        }
        return src;
    }

    public static List<Object> Load(List<TableDefine.Type> typs, byte[] src, int offset) {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < typs.size(); i++) {
            switch (typs.get(i)) {
                case INTEGER:
                    result.add(BytesToInt(src, offset+1));
                    offset += 4;
                    break;
                case FLOAT:
                    result.add(BytesToFloat(src, offset+1));
                    offset += 4;
                    break;
                case VARCHAR:
                    result.add(BytesToString(src, offset+1));
                    offset += BytesToInt(src, offset+1) + 4;
                    break;
            }
        }
        return result;
    }

}
