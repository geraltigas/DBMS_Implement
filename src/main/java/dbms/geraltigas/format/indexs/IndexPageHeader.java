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
    int nextPage = -1;

    public static final int INDEX_PAGE_HEADER_LENGTH = 8;

    public IndexPageHeader(int indexNum, int nextPage) {
        this.indexNum = indexNum;
        this.nextPage = nextPage;
    }

    public IndexPageHeader(){}

    public IndexPageHeader(byte[] header) {
        indexNum = DataDump.bytesToInt(header, 0);
        nextPage = DataDump.bytesToInt(header, 4);
    }

    public IndexPageHeader(IndexPageHeader indexPageHeader) {
        this.indexNum = indexPageHeader.indexNum;
        this.nextPage = indexPageHeader.nextPage;
    }

    public byte[] ToBytes() {
        byte[] data;
        data = DataDump.dump(List.of(TableDefine.Type.INTEGER,TableDefine.Type.INTEGER), List.of(indexNum,nextPage));
        return data;
    }
}
