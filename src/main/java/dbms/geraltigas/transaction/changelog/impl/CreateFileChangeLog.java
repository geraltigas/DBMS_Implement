package dbms.geraltigas.transaction.changelog.impl;

import dbms.geraltigas.transaction.changelog.ChangeLog;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateFileChangeLog extends ChangeLog {
    private String filePath;

    public CreateFileChangeLog(String filePath) {
        this.filePath = filePath;
        super.changeType = ChangeType.CREATE_FILE;
    }

    @Override
    public void recover() {
        Path file = Paths.get(filePath);
        if (file.toFile().exists()) {
            file.toFile().delete();
        }
    }
}
