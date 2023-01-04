package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.buffer.PageBuffer;
import dbms.geraltigas.dataccess.DiskManager;
import dbms.geraltigas.transaction.changelog.ChangeLog;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateFileChangeLog extends ChangeLog {
    private String filePath;

    @Autowired
    PageBuffer pageBuffer;

    public CreateFileChangeLog(String filePath) {
        this.filePath = filePath;
        super.changeType = ChangeType.CREATE_FILE;
        ApplicationContextUtils.autowire(this);
    }

    @Override
    public void recover() {
        Path file = Paths.get(filePath);

        String type = file.getFileName().toString().split("\\.")[1];

        switch (type) {
            case "tbl":
                String tableName = file.getFileName().toString().split("\\.")[0];
                pageBuffer.deleteRelatedPage(tableName, DiskManager.AccessType.TABLE,null);
                break;
            case "idx":
                String indexName = file.getFileName().toString().split("[\\[\\]]")[0];
                String tableNameT = file.getFileName().toString().split("[\\[\\]]")[1].split("\\(]")[0];
                pageBuffer.deleteRelatedPage(tableNameT, DiskManager.AccessType.INDEX,indexName);
                break;
            default:
                break;
        }

        if (file.toFile().exists()) {
            file.toFile().delete();
            System.out.println("[ChangeLog] rollback file :" + filePath);
        }
    }
}
