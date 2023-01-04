package dbms.geraltigas.dataccess;

import dbms.geraltigas.buffer.PageBuffer;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.exception.BlockException;
import dbms.geraltigas.exception.DataDirException;
import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.format.indexs.IndexPageHeader;
import dbms.geraltigas.format.tables.PageHeader;
import dbms.geraltigas.format.tables.TableHeader;
import dbms.geraltigas.utils.Printer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static dbms.geraltigas.buffer.PageBuffer.BLOCK_SIZE;
import static dbms.geraltigas.utils.Printer.DEBUG;


@Component
public class DiskManager {

    @Autowired
    PageBuffer pageBuffer;
    @Autowired
    ExecuteEngine executeEngine;

    @Autowired
    private TableBuffer tableBuffer;

    public TableHeader getTableHeader(String tableName) throws BlockException, DataDirException, IOException {
        byte[] header = getBytesAt(tableName,AccessType.TABLE ,null,0, TableHeader.TABLE_HEADER_LENGTH);
        TableHeader tableHeader = new TableHeader(header);
        return tableHeader;
    }

    public void setTableHeader(String tableName, TableHeader tableHeader) throws DataDirException {
        File file = new File(executeEngine.getDateDir() + "/tables/" + tableName+".tbl");
        if (!file.exists()) {
            throw new DataDirException("Table file not exists");
        }
        byte[] data = tableHeader.ToBytes();
        try {
            setBytesAt(tableName,AccessType.TABLE,null, data, 0);
        } catch (BlockException | IOException e) {
            e.printStackTrace();
        }
    }

    public void setIndexPageHeader(String tableName, String indexName, int pageIndex, IndexPageHeader indexPageHeader) throws BlockException, IOException {
        setBytesAt(tableName, AccessType.INDEX, indexName, indexPageHeader.ToBytes(), (long) pageIndex * BLOCK_SIZE);
    }

    public IndexPageHeader getIndexPageHeader(String tableName, String indexName, int pageIndex) throws BlockException, IOException, DataDirException {
        byte[] header = getBytesAt(tableName, AccessType.INDEX, indexName, (long) pageIndex * BLOCK_SIZE, IndexPageHeader.INDEX_PAGE_HEADER_LENGTH);
        return new IndexPageHeader(header);
    }

    public IndexHeader getIndexHeader(String tableName, String indexName) throws BlockException, DataDirException, IOException {
        byte[] data = getBytesAt(tableName,AccessType.INDEX,indexName, 0, IndexHeader.INDEX_HEADER_LENGTH);
        return new IndexHeader(data);
    }

    public void setIndexHeader(String tableName, String indexName, IndexHeader indexHeader) throws BlockException, IOException {
        setBytesAt(tableName,AccessType.INDEX,indexName, indexHeader.ToBytes(), 0);
    }

    public void clearHoles() throws BlockException, DataDirException, IOException {
        Path path = Path.of(executeEngine.getDateDir()).resolve("tables");
        String[] tables = Arrays.stream(path.toFile().list()).map(s -> s.replace(".tbl","")).toArray(String[]::new);
        for (String table : tables) {
            TableHeader tableHeader = getTableHeader(table);
            List<byte[]> validRecord = new ArrayList<>();
            for (int i = 0; i < tableHeader.getTableLength(); i++) {
                PageHeader pageHeader = getPageHeader(table, i+1);
                for (int j = 0; j < pageHeader.getRecordNum(); j++) {
                    byte[] record = getOneRecord(table, i+1, j);
                    if (record.length == 0 ) {
                        return;
                    }
                    if (record[0] == 1) {
                        validRecord.add(record);
                    }
                }
                int holeNum = pageHeader.getRecordNum() - validRecord.size();
                pageHeader.setRecordNum(validRecord.size());
                setPageHeader(table, i+1, pageHeader);
                for (int j = 0; j < validRecord.size(); j++) {
                    setOneRecord(table, i+1, j, validRecord.get(j));
                }
                if (holeNum > 0) {
                    Printer.print("Clear " + holeNum +" holes in table " + table + " at pageIdx " + (i+1),-1);
                }
                validRecord.clear();
            }

            // clear index holes
            Path indexPath = Path.of(executeEngine.getDateDir()).resolve("indexes").resolve(table);
            String[] indexesName = indexPath.toFile().list();
            if (indexesName == null) {
                continue;
            }
            String[] indexes = Arrays.stream(indexesName).map(s -> s.split("\\[")[0]).toArray(String[]::new);
            if (indexes  == null) {
                continue;
            }
            for (String index : indexes) {
                IndexHeader indexHeader = getIndexHeader(table, index);
                List<byte[]> validIndex = new ArrayList<>();
                for (int i = 0; i < indexHeader.getIndexHashArraySize(); i++) {
                    IndexPageHeader indexPageHeader = getIndexPageHeader(table, index, i+1);
                    for (int j = 0; j < indexPageHeader.getIndexNum(); j++) {
                        byte[] indexData = getOneIndexData(table, i+1, index, j, indexHeader.getIndexDataLength());
                        if (indexData[0] == 1) {
                            validIndex.add(indexData);
                        }
                    }
                    int holeNum = indexPageHeader.getIndexNum() - validIndex.size();
                    indexPageHeader.setIndexNum(validIndex.size());
                    setIndexPageHeader(table, index, i+1, indexPageHeader);
                    for (int j = 0; j < validIndex.size(); j++) {
                        setOneIndexData(table, i+1, index, j, indexHeader.getIndexDataPageNum() ,validIndex.get(j));
                    }
                    if (holeNum > 0) {
                        Printer.print("Clear " + holeNum +" holes in index " + index + " at pageIdx " + (i+1),-1);
                    }
                    validIndex.clear();
                }
            }
        }
    }

    public enum AccessType {
        TABLE,
        INDEX,
        BULK
    }

    private byte[] getPage(String tableName, int index) throws BlockException, DataDirException, IOException {
        return getBytesAt(tableName, AccessType.TABLE, null, index * BLOCK_SIZE, BLOCK_SIZE);
    }

    private void setPage(String tableName, int index, int offset, byte[] data) throws BlockException, IOException {
        setBytesAt(tableName, AccessType.TABLE, null, data, index * BLOCK_SIZE + offset);
    }

    public byte[] getBytesAt(String tableName, AccessType type, String appendPath, long offset, int length) throws IOException, DataDirException, BlockException { // using page buffer to read byte array include header
        File file = new File(executeEngine.getDateDir() + "/tables/" + tableName+".tbl");
        if (!file.exists()) {
            throw new DataDirException("Table " + tableName + " does not exist");
        }
        int blockId = (int) (offset / BLOCK_SIZE);
        int blockOffset = (int) (offset % BLOCK_SIZE);
        int blockEnd = (int) (offset + length);
        int blockEndIndex = blockEnd / BLOCK_SIZE;
        int blockEndOffset = blockEnd % BLOCK_SIZE;
        byte[] data = new byte[length];
        ArrayList<PageBuffer.Page> pages = new ArrayList<>(blockEndIndex - blockId + 1);
        for (int i = blockId; i <= blockEndIndex; i++) {
            PageBuffer.Page page = pageBuffer.getPage(tableName, type, appendPath , i);
            pages.add(page);
        }
        for (int i = 0; i < pages.size(); i++) {
            PageBuffer.Page page = pages.get(i);
            int start = 0;
            int end = BLOCK_SIZE;
            if (i == 0) {
                start = blockOffset;
            }
            if (i == pages.size() - 1) {
                end = blockEndOffset;
            }
            System.arraycopy(page.data, start, data, i * BLOCK_SIZE, end - start);
        }
        return data;
    }

    public void setBytesAt(String tableName, AccessType type, String appendName, byte[] data, long offset) throws BlockException, IOException {
        int blockIndex = (int) (offset / BLOCK_SIZE);
        int blockOffset = (int) (offset % BLOCK_SIZE);
        int blockEnd = (int) (offset + data.length);
        int blockEndIndex = (int) (blockEnd / BLOCK_SIZE);
        int blockEndOffset = (int) (blockEnd % BLOCK_SIZE);
        ArrayList<PageBuffer.Page> pages = new ArrayList<>(blockEndIndex - blockIndex + 1);
        for (int i = blockIndex; i <= blockEndIndex; i++) {
            PageBuffer.Page page = pageBuffer.getPage(tableName,type,appendName, i);
            pages.add(page);
        }
        for (int i = 0; i < pages.size(); i++) {
            PageBuffer.Page page = pages.get(i);
            if (i == 0) {
                page.writeBytes(data, blockOffset, Math.min(BLOCK_SIZE - blockOffset, data.length));
                pageBuffer.setPage(tableName,type,appendName, page.blockId, page, true);
            } else if (i == pages.size() - 1) {
                page.writeBytes(data, 0, blockEndOffset);
                pageBuffer.setPage(tableName,type,appendName, page.blockId, page, true);
            } else {
                page.writeBytes(data, 0, BLOCK_SIZE);
                pageBuffer.setPage(tableName,type,appendName, page.blockId, page, true);
            }
        }
        pageBuffer.FlushPages(pages);
    }

    public byte[] getOneRecord(String tableName, int pageIndex, int recordIndex) throws BlockException, DataDirException, IOException {
        byte[] pageData = getPage(tableName, pageIndex);
        PageHeader pageHeader = new PageHeader(pageData);
        int lastRecordOffset = pageHeader.getLastRecordOffset();
        byte[] record = new byte[pageHeader.getRecordLength()];
        System.arraycopy(pageData, lastRecordOffset + ((pageHeader.getRecordNum()-1)-recordIndex) * pageHeader.getRecordLength(), record, 0, record.length);
        return record;
    }

    public void setOneRecord(String tableName, int pageIndex, int recordIndex, byte[] record) throws BlockException, DataDirException, IOException {
        byte[] pageData = getPage(tableName, pageIndex);
        PageHeader pageHeader = new PageHeader(pageData);
        int lastRecordOffset = pageHeader.getLastRecordOffset();
        System.arraycopy(record, 0, pageData, lastRecordOffset + ((pageHeader.getRecordNum()-1)-recordIndex) * pageHeader.getRecordLength(), record.length);
        setPage(tableName, pageIndex, 0, pageData);
    }

    public PageHeader getPageHeader(String tableName, int pageIndex) throws BlockException, DataDirException, IOException {
        DEBUG("in getPageHeader");
        byte[] pageData = getPage(tableName, pageIndex);
        DEBUG("in getPageHeader2");

        return new PageHeader(pageData);
    }

    public void setPageHeader(String tableName, int pageIndex, PageHeader pageHeader) throws BlockException, DataDirException, IOException {
        byte[] pageData = getPage(tableName, pageIndex);
        System.arraycopy(pageHeader.ToBytes(), 0, pageData, 0, PageHeader.PAGE_HEADER_LENGTH);
        setPage(tableName, pageIndex, 0, pageData);
    }

//    public void setRecord(String tableName, int pageIndex, int index, byte[] record, int recordLength, PageHeader pageHeader) {
//        long offset = (long) (pageIndex) * BLOCK_SIZE + (long) index * recordLength + pageHeader.getLastRecordOffset();
//        try {
//            setBytesAt(tableName,AccessType.TABLE,null, record, offset);
//        } catch (BlockException | IOException e) {
//            e.printStackTrace();
//        }
//    }

    public byte[] getOneIndexData(String tableName, int pageIndex, String indexName, int index, int recordLength) throws BlockException, DataDirException, IOException {
        return getBytesAt(tableName, AccessType.INDEX, indexName, (long) (pageIndex) * BLOCK_SIZE + (long) index * recordLength + IndexPageHeader.INDEX_PAGE_HEADER_LENGTH, recordLength);
    }

    public void setOneIndexData(String tableName, int pageIndex, String indexName, int index, int recordLength, byte[] data) throws BlockException, IOException {
        setBytesAt(tableName, AccessType.INDEX, indexName, data, (long) (pageIndex) * BLOCK_SIZE + (long) index * recordLength + IndexPageHeader.INDEX_PAGE_HEADER_LENGTH);
    }

}
