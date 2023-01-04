package dbms.geraltigas.format.indexs;

import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.utils.DataDump;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class IndexHeader {
    int indexHashArraySize = 1024;
    int indexDataPageNum = 1024; // the page number of the index data part

    int indexDataLength = 0;
    public static final int INDEX_HEADER_LENGTH = 12;

    public IndexHeader(int indexDataLength) {
        this.indexDataLength = indexDataLength;
    }

    public IndexHeader(byte[] header) {
        indexHashArraySize = DataDump.bytesToInt(header, 0);
        indexDataPageNum = DataDump.bytesToInt(header, 4);
        indexDataLength = DataDump.bytesToInt(header, 8);
    }

    public byte[] ToBytes() {
        byte[] data;
        data = DataDump.dump(List.of(TableDefine.Type.INTEGER, TableDefine.Type.INTEGER, TableDefine.Type.INTEGER), List.of(indexHashArraySize, indexDataPageNum,indexDataLength));
        return data;
    }
}
