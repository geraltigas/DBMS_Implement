package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.format.indexs.IndexHeader;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import net.sf.jsqlparser.statement.create.table.Index;

public class IndexHeaderChangeLog extends ChangeLog {
    String tableName;
    String indexName;
    IndexHeader oldIndexHeader;

    public IndexHeaderChangeLog(String tableName, String indexName, IndexHeader oldIndexHeader) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.oldIndexHeader = oldIndexHeader;
        super.changeType = ChangeType.INDEX_HEADER;
    }

    @Override
    public void recover() {

    }
}
