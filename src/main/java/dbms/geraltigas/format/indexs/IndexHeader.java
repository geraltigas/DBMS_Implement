package dbms.geraltigas.format.indexs;

import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.utils.DataDump;

import java.util.List;

public class IndexHeader {
    int indexHashArraySize = 1024;
    int indexDataPageNum = 1024; // the page number of the index data part

    public static final int INDEX_HEADER_LENGTH = 8;

    public IndexHeader(int indexHashArraySize, int indexDataPageNum) {
        this.indexHashArraySize = indexHashArraySize;
        this.indexDataPageNum = indexDataPageNum;
    }

    public IndexHeader(byte[] header) {
        indexHashArraySize = DataDump.bytesToInt(header, 0);
        indexDataPageNum = DataDump.bytesToInt(header, 4);
    }

    public byte[] ToBytes() {
        byte[] data;
        data = DataDump.dump(List.of(TableDefine.Type.INTEGER, TableDefine.Type.INTEGER), List.of(indexHashArraySize, indexDataPageNum));
        return data;
    }
}
