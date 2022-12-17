package dbms.geraltigas.format.tables;

import dbms.geraltigas.utils.DataDump;
import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableHeader { // in the first block of the table file
    private int tableLength = 1; // the number of the block the table file
    private int endOffset = TABLE_HEADER_LENGTH; // the offset of the last + 1 record in the last block
    private int bulkNum = 0; // the number of the bulk

    public static final int TABLE_HEADER_LENGTH = 12;

    public TableHeader(byte[] header) {
        tableLength = DataDump.BytesToInt(header, 0);
        endOffset = DataDump.BytesToInt(header, 4);
        bulkNum = DataDump.BytesToInt(header, 8);
    }

    public byte[] ToBytes() {
        byte[] data;
        data = DataDump.Dump(List.of(TableDefine.Type.INTEGER, TableDefine.Type.INTEGER, TableDefine.Type.INTEGER), List.of(tableLength, endOffset, bulkNum));
        return data;
    }

    public int getHeaderLength() {
        return TABLE_HEADER_LENGTH;
    }

}
