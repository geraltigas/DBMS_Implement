package dbms.geraltigas.expression;

import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.utils.DataDump;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.awt.dnd.DropTarget;
import java.util.*;

import static dbms.geraltigas.expression.Expression.Op.LONG_VALUE;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Expression {

    public boolean evalNoAlias(byte[] record, TableDefine tableDefine) throws DataTypeException {
        Map<String,Object> map = new HashMap<>();
        Map<String, TableDefine.Type> typeMap = new HashMap<>();
        List<Object> datas = DataDump.load(tableDefine.getColTypes(),record,0);
        for (int i = 0; i < tableDefine.getColNames().size(); i++) {
            String field = tableDefine.getColNames().get(i);
            map.put(field,datas.get(i));
            typeMap.put(field, tableDefine.getColTypes().get(i));
        }
        return (Boolean)this.eval(typeMap,map);
    }

    public static void nullEval(byte[] record, TableDefine tableDefine, List<String> alias, List<Expression> expressions, List<String> res) throws DataTypeException {
        List<TableDefine.Type> types = tableDefine.getColTypes();
        List<String> names = tableDefine.getColNames();
        List<Object> values = DataDump.load(types, record, 0);
        Map<String,Object> map = new HashMap<>();
        Map<String, TableDefine.Type> typeMap = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i),values.get(i));
            typeMap.put(names.get(i),types.get(i));
        }

        evalAliasExpression(typeMap,alias,expressions,map);
        for (String s : alias) {
            if (map.get(s.trim()) == null) {
                throw new DataTypeException("Field " + s.trim() + " not found");
            }
        }
        String[] valueS = alias.stream().map(item -> map.get(item.trim()).toString()).toArray(String[]::new);
        for (int i = 0; i < valueS.length; i++) {
            valueS[i] = String.format("%-10s",valueS[i]);
        }
        res.add("|"+String.join("|",valueS)+"|");
    }

    public static void nullEvalAll(List<Object> values, List<TableDefine.Type> types,List<String> names, List<String> alias, List<Expression> expressions, List<String> res) throws DataTypeException {
        Map<String,Object> map = new HashMap<>();
        Map<String, TableDefine.Type> typeMap = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i),values.get(i));
            typeMap.put(names.get(i),types.get(i));
        }

        evalAliasExpression(typeMap,alias,expressions,map);
        String[] valueS = names.stream().map(item -> map.get(item).toString()).toArray(String[]::new);
        for (int i = 0; i < valueS.length; i++) {
            valueS[i] = String.format("%-10s",valueS[i]);
        }
        res.add("|"+String.join("|",valueS)+"|");
    }

    public boolean eval(byte[] record, TableDefine tableDefine, List<String> alias, List<Expression> expressions, List<String> res) throws DataTypeException {
        List<TableDefine.Type> types = tableDefine.getColTypes();
        List<String> names = tableDefine.getColNames();
        List<Object> values = DataDump.load(types, record, 0);
        Map<String,Object> map = new HashMap<>();
        Map<String, TableDefine.Type> typeMap = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i),values.get(i));
            typeMap.put(names.get(i),types.get(i));
        }

        evalAliasExpression(typeMap,alias,expressions,map);
        for (String key : alias) {
            if (map.get(key.trim()) == null) {
                throw new DataTypeException("Field "+key.trim()+" not found");
            }
        }
        if ((Boolean) this.eval(typeMap,map)) {
            String[] valueS = alias.stream().map(item -> map.get(item.trim()).toString()).toArray(String[]::new);
            for (int i = 0; i < valueS.length; i++) {
                valueS[i] = String.format("%-10s",valueS[i]);
            }
            res.add("|"+String.join("|",valueS)+"|");
            return true;
        }
        return false;
    }

    public boolean evalAll(List<Object> values, List<TableDefine.Type> types,List<String> names, List<String> alias, List<Expression> expressions, List<String> res) throws DataTypeException {
        Map<String,Object> map = new HashMap<>();
        Map<String, TableDefine.Type> typeMap = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i),values.get(i));
            typeMap.put(names.get(i),types.get(i));
        }

        evalAliasExpression(typeMap,alias,expressions,map);

        if ((Boolean) this.eval(typeMap,map)) {
            String[] valueS = alias.stream().map(item -> map.get(item.trim()).toString()).toArray(String[]::new);
            for (int i = 0; i < valueS.length; i++) {
                valueS[i] = String.format("%-10s",valueS[i]);
            }
            res.add("|"+String.join("|",valueS)+"|");
            return true;
        }
        return false;
    }



    public static void evalAliasExpression(Map<String, TableDefine.Type> typeMap, List<String> alias, List<Expression> expressions, Map<String, Object> map) throws DataTypeException {
        for (int i = 0; i < alias.size(); i++) {
            map.putIfAbsent(alias.get(i).trim(),expressions.get(i).eval(typeMap,map));
            Object o = map.get(alias.get(i).trim());
            TableDefine.Type type = null;
            switch (o.getClass().getSimpleName()){
                case "Integer" -> {
                    type = TableDefine.Type.INTEGER;
                }
                case "Float" -> {
                    type = TableDefine.Type.FLOAT;
                }
                case "String" -> {
                    type = TableDefine.Type.VARCHAR;
                }
            }
            typeMap.putIfAbsent(alias.get(i).trim(),type);
        }
    }

    private Object eval(Map<String, TableDefine.Type> typeMap, Map<String,Object> map) throws DataTypeException {
        return switch (op) {
            case AND:{
                Object leftValue = left.eval(typeMap,map);
                Object rightValue = right.eval(typeMap,map);
                yield (Boolean) leftValue && (Boolean) rightValue;
            }
            case OR :{
                Object leftValue = left.eval(typeMap, map);
                Object rightValue = right.eval(typeMap, map);
                yield (Boolean) leftValue || (Boolean) rightValue;
            }
            case SUBTRACT: {
                Object leftValue = left.eval(typeMap, map);
                Object rightValue = right.eval(typeMap, map);
                switch (typeMap.get(left.name)) {
                    case INTEGER -> {
                        if (!(rightValue instanceof Integer || rightValue instanceof Float)) {
                            throw new DataTypeException("Intger type can not be subtracted with a non-numeric type");
                        }
                        if (rightValue instanceof Integer) {
                            yield (Integer) leftValue - (Integer) rightValue;
                        } else {
                            yield (Integer) leftValue - (Float) rightValue;
                        }
                    }
                    case FLOAT -> {
                        if (!(rightValue instanceof Integer || rightValue instanceof Float)) {
                            throw new DataTypeException("Float type can not be subtracted with a non-numeric type");
                        }
                        if (rightValue instanceof Integer) {
                            yield (Float) leftValue - (Integer) rightValue;
                        } else {
                            yield (Float) leftValue - (Float) rightValue;
                        }
                    }
                    default -> throw new DataTypeException("Only integer and float types can be subtracted");
                }
            }
            case PLUS:{
                Object leftValue = left.eval(typeMap, map);
                Object rightValue = right.eval(typeMap, map);
                switch (typeMap.get(left.name)) {
                    case INTEGER -> {
                        if (!(rightValue instanceof Integer || rightValue instanceof Float)) {
                            throw new DataTypeException("Integer type can not be plused with a non-numeric type");
                        }
                        if (rightValue instanceof Integer) {
                            yield (Integer) leftValue + (Integer) rightValue;
                        } else {
                            yield (Integer) leftValue + (Float) rightValue;
                        }
                    }
                    case FLOAT -> {
                        if (!(rightValue instanceof Integer || rightValue instanceof Float)) {
                            throw new DataTypeException("Float type can not be plused with a non-numeric type");
                        }
                        if (rightValue instanceof Integer) {
                            yield (Float) leftValue + (Integer) rightValue;
                        } else {
                            yield (Float) leftValue + (Float) rightValue;
                        }
                    }
                    default -> throw new DataTypeException("Only integer and float types can be plused");
                }
            }
            case EQUAL:{
                Object leftValue = left.eval(typeMap, map);
                Object rightValue = right.eval(typeMap, map);
                switch (typeMap.get(left.name)) {
                    case INTEGER -> {
                        if (!(leftValue instanceof Integer && rightValue instanceof Integer)) {
                            throw new DataTypeException("Integer type can not compare with other type");
                        }
                        yield (Integer) leftValue - (Integer) rightValue == 0;
                    }
                    case FLOAT -> {
                        if (!(leftValue instanceof Float && rightValue instanceof Float)) {
                            throw new DataTypeException("Float type can not compare with other type");
                        }
                        yield (Float) leftValue - (Float) rightValue == 0;
                    }
                    case VARCHAR -> {
                        if (!(leftValue instanceof String && rightValue instanceof String)) {
                            throw new DataTypeException("String type can not compare with other type");
                        }
                        yield Objects.equals((String) leftValue, (String) rightValue);
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + typeMap.get(leftValue.getClass().getName()));
                }
            }
            case NULL:{
                if (map.containsKey(name)) {
                    yield map.get(name);
                }else {
                    throw new DataTypeException("Field "+name+" not found in Expression Evaluation Cache, please make sure calculate the expression in order");
                }
            }
            case LONG_VALUE:{
                name = getRandomString(10);
                typeMap.put(name, TableDefine.Type.INTEGER);
                yield ((Long)value).intValue();
            }
            case DOUBLE_VALUE:{
                name = getRandomString(10);
                typeMap.put(name, TableDefine.Type.FLOAT);
                yield ((Double)value).floatValue();
            }
            case STRING_VALUE:{
                name = getRandomString(10);
                typeMap.put(name, TableDefine.Type.VARCHAR);
                yield value;
            }
        };
    }
    private static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public enum Op {
        OR,
        AND,
//        SUBTRACT,
//        PLUS,
        NULL, // MEANS COLUMN
        EQUAL,
        LONG_VALUE,
        DOUBLE_VALUE,
        STRING_VALUE,
        SUBTRACT,
        PLUS
    }
    Expression left;
    Expression right;
    String name;
    Object value;
    Op op;
}
