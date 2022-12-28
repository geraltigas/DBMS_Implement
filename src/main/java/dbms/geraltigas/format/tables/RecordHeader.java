package dbms.geraltigas.format.tables;

import dbms.geraltigas.utils.DataDump;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;

@Getter
@Setter
public class RecordHeader {
    private byte valid;
    private int index;

    public static final int RECORD_HEADER_SIZE = 5;

    public RecordHeader(byte valid, int index) {
        this.valid = valid;
        this.index = index;
    }

    public RecordHeader(byte[] data) {
        this.valid = data[0];
        this.index = DataDump.bytesToInt(data, 1);
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[9];
        bytes[0] = valid;
        System.arraycopy(DataDump.intToBytes(index), 0, bytes, 1, 4);
        return bytes;
    }
}


