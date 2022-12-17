package dbms.geraltigas.format.tables;

import dbms.geraltigas.dataccess.DiskManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public class Bulk {

    @Autowired
    DiskManager diskManager;

    private String tableName;
    private Map<String,Integer> bulkContent;
    private int bulkNum;

    public Bulk(String tableName, Map<String,Integer> bulkContent, int bulkNum) {
        this.tableName = tableName;
        this.bulkContent = bulkContent;
        this.bulkNum = bulkNum;
    }
}
