package dbms.geraltigas.format.tables;

import dbms.geraltigas.utils.DataDump;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;


@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class PageHeader {
    private int recordNum = 0;
    private int recordLength = 0;
    private int lastRecordOffset = 0;

    public static final int PAGE_HEADER_LENGTH = 12;

    public PageHeader(byte[] header) {
        recordNum = DataDump.bytesToInt(header, 0);
        recordLength = DataDump.bytesToInt(header, 4);
        lastRecordOffset = DataDump.bytesToInt(header, 8);
    }

    public byte[] ToBytes() {
        byte[] data;
        data = DataDump.dump(List.of(TableDefine.Type.INTEGER, TableDefine.Type.INTEGER, TableDefine.Type.INTEGER), List.of(recordNum, recordLength, lastRecordOffset));
        return data;
    }
}
