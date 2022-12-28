package dbms.geraltigas.dataccess;

import dbms.geraltigas.buffer.BlockBuffer;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.tables.PageHeader;
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
    ExecuteEngine executeEngine;

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

    public byte[] readPage(String tableName, int index) throws BlockException, DataDirException, IOException {
        return readBytesAt(tableName, AccessType.TABLE, null, index * BlockBuffer.BLOCK_SIZE, BlockBuffer.BLOCK_SIZE);
    }

    public void writePage(String tableName, int index,int offset, byte[] data) throws BlockException, IOException {
        writeBytesAt(tableName, AccessType.TABLE, null, data, index * BlockBuffer.BLOCK_SIZE + offset);
    }

    public byte[] readBytesAt(String tableName, AccessType type, String appendPath, long offset, int length) throws IOException, DataDirException, BlockException { // using page buffer to read byte array include header
        File file = new File(executeEngine.getDateDir() + "/tables/" + tableName+".tbl");
        if (!file.exists()) {
            throw new DataDirException("Table " + tableName + " does not exist");
        }
        int blockId = (int) (offset / BlockBuffer.BLOCK_SIZE);
        int blockOffset = (int) (offset % BlockBuffer.BLOCK_SIZE);
        int blockEnd = (int) (offset + length);
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
        File file = new File(executeEngine.getDateDir() + "/tables/" + tableName+".tbl");
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

    public byte[] readRecords(String tableName,int pageIndex) throws BlockException, DataDirException, IOException { // TODO: fix here
        byte[] pageData = readPage(tableName, pageIndex);
        PageHeader pageHeader = new PageHeader(pageData);
        int lastRecordOffset = pageHeader.getLastRecordOffset();
        byte[] records = new byte[pageHeader.getRecordNum() * pageHeader.getRecordLength()];
        System.arraycopy(pageData, lastRecordOffset, records, 0, records.length);
        return records;
    }

    public byte[] readOneRecord(String tableName, int pageIndex, int recordIndex) throws BlockException, DataDirException, IOException {
        byte[] pageData = readPage(tableName, pageIndex);
        PageHeader pageHeader = new PageHeader(pageData);
        int lastRecordOffset = pageHeader.getLastRecordOffset();
        byte[] record = new byte[pageHeader.getRecordLength()];
        System.arraycopy(pageData, lastRecordOffset + ((pageHeader.getRecordNum()-1)-recordIndex) * pageHeader.getRecordLength(), record, 0, record.length);
        return record;
    }

    public void writeOneRecord(String tableName, int pageIndex, int recordIndex, byte[] record) throws BlockException, DataDirException, IOException {
        byte[] pageData = readPage(tableName, pageIndex);
        PageHeader pageHeader = new PageHeader(pageData);
        int lastRecordOffset = pageHeader.getLastRecordOffset();
        System.arraycopy(record, 0, pageData, lastRecordOffset + ((pageHeader.getRecordNum()-1)-recordIndex) * pageHeader.getRecordLength(), record.length);
        writePage(tableName, pageIndex, 0, pageData);
    }

    public PageHeader readPageHeader(String tableName, int pageIndex) throws BlockException, DataDirException, IOException {
        byte[] pageData = readPage(tableName, pageIndex);
        return new PageHeader(pageData);
    }

    public void writePageHeader(String tableName, int pageIndex, PageHeader pageHeader) throws BlockException, DataDirException, IOException {
        byte[] pageData = readPage(tableName, pageIndex);
        System.arraycopy(pageHeader.ToBytes(), 0, pageData, 0, PageHeader.PAGE_HEADER_LENGTH);
        writePage(tableName, pageIndex, 0, pageData);
    }

    public void writeRecord(String tableName,int pageIndex, int index, byte[] record, int recordLength,PageHeader pageHeader) {
        long offset = (long) (pageIndex) * BlockBuffer.BLOCK_SIZE + (long) index * recordLength + pageHeader.getLastRecordOffset();
        try {
            writeBytesAt(tableName,AccessType.TABLE,null, record, offset);
        } catch (BlockException | IOException e) {
            e.printStackTrace();
        }
    }

    public IndexHeader getIndexHeader(String tableName, String indexName) throws BlockException, DataDirException, IOException {
        byte[] data = readBytesAt(tableName,AccessType.INDEX,indexName, 0, IndexHeader.INDEX_HEADER_LENGTH);
        return new IndexHeader(data);
    }
}
