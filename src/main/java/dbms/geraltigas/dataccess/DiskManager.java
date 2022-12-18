package dbms.geraltigas.dataccess;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.tables.TableHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;


@Component
public class DiskManager {

    @Autowired
    BlockBuffer blockBuffer;
    @Autowired
    ExecList execList;

    public TableHeader getTableHeader(String tableName) throws BlockException, DataDirException, IOException {
        byte[] header = readBytesAt(tableName,AccessType.TABLE ,null,0, TableHeader.TABLE_HEADER_LENGTH);
        TableHeader tableHeader = new TableHeader(header);
        return tableHeader;
    }

    public void setTableHeader(String tableName, TableHeader tableHeader) throws BlockException, IOException {
        writeBytesAt(tableName, AccessType.TABLE,null, tableHeader.ToBytes(), 0);
    }

    public enum AccessType {
        TABLE,
        INDEX,
        BULK
    }

    public byte[] readBytesAt(String tableName, AccessType type, String appendPath, long offset, int length) throws IOException, DataDirException, BlockException { // using page buffer to read byte array include header
        File file = new File(execList.getDateDir() + "/tables/" + tableName+".tbl");
        if (!file.exists()) {
            throw new DataDirException("Table " + tableName + " does not exist");
        }
        int blockId = (int) (offset / BlockBuffer.BLOCK_SIZE);
        int blockOffset = (int) (offset % BlockBuffer.BLOCK_SIZE);
        int blockEnd = blockOffset + length;
        int blockEndIndex = blockEnd / BlockBuffer.BLOCK_SIZE;
        int blockEndOffset = blockEnd % BlockBuffer.BLOCK_SIZE;
        byte[] data = new byte[length];
        ArrayList<BlockBuffer.Page> pages = new ArrayList<>(blockEndIndex - blockId + 1);
        for (int i = blockId; i <= blockEndIndex; i++) {
            BlockBuffer.Page page = blockBuffer.getPage(tableName, type, appendPath , i);
            pages.add(page);
        }
        for (int i = 0; i < pages.size(); i++) {
            BlockBuffer.Page page = pages.get(i);
            int start = 0;
            int end = BlockBuffer.BLOCK_SIZE;
            if (i == 0) {
                start = blockOffset;
            }
            if (i == pages.size() - 1) {
                end = blockEndOffset;
            }
            System.arraycopy(page.data, start, data, i * BlockBuffer.BLOCK_SIZE, end - start);
        }
        return data;
    }

    public void writeBytesAt(String tableName, AccessType type, String appendName, byte[] data, long offset) throws BlockException, IOException {
        int blockIndex = (int) (offset / BlockBuffer.BLOCK_SIZE);
        int blockOffset = (int) (offset % BlockBuffer.BLOCK_SIZE);
        int blockEnd = (int) (offset + data.length);
        int blockEndIndex = (int) (blockEnd / BlockBuffer.BLOCK_SIZE);
        int blockEndOffset = (int) (blockEnd % BlockBuffer.BLOCK_SIZE);
        ArrayList<BlockBuffer.Page> pages = new ArrayList<>(blockEndIndex - blockIndex + 1);
        for (int i = blockIndex; i <= blockEndIndex; i++) {
            BlockBuffer.Page page = blockBuffer.getPage(tableName,type,appendName, i);
            pages.add(page);
        }
        for (int i = 0; i < pages.size(); i++) {
            BlockBuffer.Page page = pages.get(i);
            if (i == 0) {
                page.writeBytes(data, blockOffset, Math.min(BlockBuffer.BLOCK_SIZE - blockOffset, data.length));
            } else if (i == pages.size() - 1) {
                page.writeBytes(data, 0, blockEndOffset);
            } else {
                page.writeBytes(data, 0, BlockBuffer.BLOCK_SIZE);
            }
        }
        blockBuffer.FlushPages(pages);
    }

    public void writeTableFileHeader(String tableName, TableHeader tableHeader) throws DataDirException {
        File file = new File(execList.getDateDir() + "/tables/" + tableName+".tbl");
        if (!file.exists()) {
            throw new DataDirException("Table file not exists");
        }
        byte[] data = tableHeader.ToBytes();
        try {
            writeBytesAt(tableName,AccessType.TABLE,null, data, 0);
        } catch (BlockException | IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readRecords(String tableName, int index, int recordNum, int recardLength, TableHeader tableHeader) throws BlockException, DataDirException, IOException {
        long offset = tableHeader.getHeaderLength() + (long) index * recardLength;
        long maxEndOffset = offset + (long) recordNum * recardLength;
        int length = Math.min((int) (maxEndOffset - offset),(int) (tableHeader.getEndOffset() - offset));
        return readBytesAt(tableName,AccessType.TABLE,null, offset, length);
    }

    public void writeRecord(String tableName, int index, byte[] record, int recordLength, TableHeader getTableHeader) {
        long offset = getTableHeader.getHeaderLength() + (long) index * recordLength;
        try {
            writeBytesAt(tableName,AccessType.TABLE,null, record, offset);
        } catch (BlockException | IOException e) {
            e.printStackTrace();
        }
    }
}
