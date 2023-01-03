package dbms.geraltigas.utils;

import dbms.geraltigas.dataccess.execplan.impl.InsertExec;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.format.tables.TableDefine;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
public class IndexUtils {
    @Getter
    List<TableDefine.Type> typeList = new ArrayList<>(3);

    @Getter
    int indexDataLength;
    List<Object> data = new ArrayList<>(3);
    public IndexUtils(TableDefine.Type type,List<String> attr) throws DataTypeException {
        typeList.add(type);
        typeList.add(TableDefine.Type.INTEGER);
        typeList.add(TableDefine.Type.INTEGER);
        List<List<String>> attrValues = new ArrayList<>();
        attrValues.add(attr);
        attrValues.add(new ArrayList<>());
        attrValues.add(new ArrayList<>());
        indexDataLength = InsertExec.CalculateLength(typeList, attrValues);
    }

    public List<Object> generateIndexDataObjectList(Object data, int pageIdx,int recordIdx) {
        this.data.clear();
        this.data.add(data);
        this.data.add(pageIdx);
        this.data.add(recordIdx);
        return this.data;
    }

    public byte[] generateIndexDataBytes(Object data, int pageIdx,int recordIdx) {
        this.data.clear();
        this.data.add(data);
        this.data.add(pageIdx);
        this.data.add(recordIdx);
        return DataDump.dumpWithValid(typeList, this.data);
    }

    public byte[] generateIndexDataBytes() {
        return DataDump.dumpWithValid(typeList, data);
    }

}
