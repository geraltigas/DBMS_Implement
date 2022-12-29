package dbms.geraltigas.utils;

import dbms.geraltigas.format.tables.TableDefine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class DataDump {

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static int bytesToInt(byte[] src, int offset) {
        int value;
        value = (int) ((src[offset + 3] & 0xFF)
                | ((src[offset + 2] & 0xFF) << 8)
                | ((src[offset + 1] & 0xFF) << 16)
                | ((src[offset] & 0xFF) << 24));
        return value;
    }

    public static byte[] floatToBytes(float value) {
        int fbit = Float.floatToIntBits(value);
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (fbit >> (24 - i * 8));
        }
        return b;
    }

    public static float bytesToFloat(byte[] src, int offset) {
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

    public static byte[] stringToBytes(String value) {
        byte[] bytes = value.getBytes();
        byte[] temp = new byte[bytes.length + 4];
        System.arraycopy(intToBytes(bytes.length), 0, temp, 0, 4);
        System.arraycopy(bytes, 0, temp, 4, bytes.length);
        return temp;
    }

    public static String bytesToString(byte[] src, int offset) {
        int len = bytesToInt(src, offset);
        return new String(src, 4+offset, len);
    }

    public static byte[] dumpWithValid(List<TableDefine.Type> typs, List<Object> datas) {
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
                    byte[] intBytes = intToBytes((int) datas.get(i));
                    System.arraycopy(intBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case FLOAT -> {
                    byte[] floatBytes = floatToBytes((float) datas.get(i));
                    System.arraycopy(floatBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case VARCHAR -> {
                    byte[] stringBytes = stringToBytes((String) datas.get(i));
                    System.arraycopy(stringBytes, 0, result, offset, stringBytes.length);
                    offset += stringBytes.length;
                }
            }
        }
        return result;
    }
    public static byte[] dump(List<TableDefine.Type> typs, List<Object> datas) {
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
                    byte[] intBytes = intToBytes((int) datas.get(i));
                    System.arraycopy(intBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case FLOAT -> {
                    byte[] floatBytes = floatToBytes((float) datas.get(i));
                    System.arraycopy(floatBytes, 0, result, offset, 4);
                    offset += 4;
                }
                case VARCHAR -> {
                    byte[] stringBytes = stringToBytes((String) datas.get(i));
                    System.arraycopy(stringBytes, 0, result, offset, stringBytes.length);
                    offset += stringBytes.length;
                }
            }
        }
        return result;
    }

    public static byte[] dumpSrc(byte[] src, int per_size, List<TableDefine.Type> typs, List<List<Object>> records) {
        for (int i = 0; i < records.size(); i++) {
            byte[] temp = dumpWithValid(typs, records.get(i));
            System.arraycopy(temp, 0, src, i * per_size, temp.length);
        }
        return src;
    }

    public static List<Object> load(List<TableDefine.Type> typs, byte[] src, int offset) {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < typs.size(); i++) {
            switch (typs.get(i)) {
                case INTEGER:
                    result.add(bytesToInt(src, offset+1));
                    offset += 4;
                    break;
                case FLOAT:
                    result.add(bytesToFloat(src, offset+1));
                    offset += 4;
                    break;
                case VARCHAR:
                    result.add(bytesToString(src, offset+1));
                    offset += bytesToInt(src, offset+1) + 4;
                    break;
            }
        }
        return result;
    }

    public static List<Object> loadAll(List<List<TableDefine.Type>> b, List<byte[]> record) {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < b.size(); i++) {
            result.addAll(load(b.get(i), record.get(i), 0));
        }
        return result;
    }
}
