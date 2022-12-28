package dbms.geraltigas.format.indexs;

import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.utils.DataDump;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class IndexPageHeader {

    @Getter
    @Setter
    int indexNum = 0;

    public static final int INDEX_PAGE_HEADER_LENGTH = 4;

    public IndexPageHeader(int indexNum) {
        this.indexNum = indexNum;
    }

    public IndexPageHeader(){}

    public IndexPageHeader(byte[] header) {
        indexNum = DataDump.bytesToInt(header, 0);
    }

    public byte[] ToBytes() {
        byte[] data;
        data = DataDump.dump(List.of(TableDefine.Type.INTEGER), List.of(indexNum));
        return data;
    }
}
