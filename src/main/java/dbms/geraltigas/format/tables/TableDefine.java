package dbms.geraltigas.format.tables;

import dbms.geraltigas.exception.DataTypeException;
import lombok.*;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static dbms.geraltigas.dataccess.execplan.impl.InsertExec.CalculateLength;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableDefine implements Serializable {
    private String tableName;
    private List<String> colNames;
    private List<Type> colTypes;
    private List<List<String>> colAttrs;


    private int recordLength;

    public enum Type{
        INTEGER,
        VARCHAR,
        FLOAT
    }


    public TableDefine(String tableName, String[] colNames, Type[] colTypes, List<String>[] colAttrs) throws DataTypeException {
        this.tableName = tableName;
        this.colNames = new LinkedList<>();
        this.colTypes = new LinkedList<>();
        this.colAttrs = new LinkedList<>();
        for (String i : colNames) {
            this.colNames.add(i);
        }
        for (Type i : colTypes) {
            this.colTypes.add(i);
        }
        for (List<String> i : colAttrs) {
            this.colAttrs.add(i);
        }
        this.recordLength = CalculateLength(this.colTypes, this.colAttrs);
    }

    public TableDefine(String tableName, String[] colNames, String[] colTypes, List<String>[] colAttrs) throws DataTypeException {
        this.tableName = tableName;
        this.colNames = new LinkedList<>();

        for (String colName : colNames) {
            this.colNames.add(colName);
        }

        this.colTypes = new LinkedList<>();

        for (String colType : colTypes) {
            switch (colType.toUpperCase()) {
                case "INT":
                    this.colTypes.add(Type.INTEGER);
                    break;
                case "VARCHAR":
                    this.colTypes.add(Type.VARCHAR);
                    break;
                default:
                    throw new DataTypeException("Unknown data type: " + colType);
            }
        }

        this.colAttrs = new LinkedList<>();
        for (List<String> colAttr : colAttrs) {
            if (colAttr == null) {
                this.colAttrs.add(List.of());
            } else {
                this.colAttrs.add(colAttr);
            }
        }
        this.recordLength = CalculateLength(this.colTypes, this.colAttrs);
    }
}
