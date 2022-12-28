package dbms.geraltigas.expression;

import dbms.geraltigas.format.tables.TableDefine;
import dbms.geraltigas.utils.DataDump;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Expression {

    public boolean evalNoAlias(byte[] record, TableDefine tableDefine) {
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

    public static void nullEval(byte[] record, TableDefine tableDefine, List<String> alias, List<Expression> expressions, List<String> res) {
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
        res.add(String.join(",",names.stream().map(item -> map.get(item).toString()).toArray(String[]::new)));
    }

    public boolean eval(byte[] record, TableDefine tableDefine, List<String> alias, List<Expression> expressions, List<String> res) {
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

        if ((Boolean) this.eval(typeMap,map)) {
            res.add(String.join(",",names.stream().map(item -> map.get(item).toString()).toArray(String[]::new)));
            return true;
        }
        return false;
    }



    private static void evalAliasExpression(Map<String, TableDefine.Type> typeMap, List<String> alias, List<Expression> expressions, Map<String, Object> map) {
        for (int i = 0; i < alias.size(); i++) {
            map.putIfAbsent(alias.get(i),expressions.get(i).eval(typeMap,map));
        }
    }

    private Object eval(Map<String, TableDefine.Type> typeMap, Map<String,Object> map) {
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
//            case SUBTRACT: {
//                Object leftValue = left.eval(typeMap, map);
//                Object rightValue = right.eval(typeMap, map);
//                switch (typeMap.get(left.name)) {
//                    case INTEGER -> {
//                        yield (Integer) leftValue - (Integer) rightValue;
//                    }
//                    case FLOAT -> {
//                        yield (Float) leftValue - (Float) rightValue;
//                    }
//                    default -> throw new IllegalStateException("Unexpected value: " + typeMap.get(leftValue.getClass().getName()));
//                }
//            }
//            case PLUS:{
//                Object leftValue = left.eval(typeMap, map);
//                Object rightValue = right.eval(typeMap, map);
//                switch (typeMap.get(left.name)) {
//                    case INTEGER -> {
//                        yield (Integer) leftValue + (Integer) rightValue;
//                    }
//                    case FLOAT -> {
//                        yield (Float) leftValue + (Float) rightValue;
//                    }
//                    default -> throw new IllegalStateException("Unexpected value: " + typeMap.get(leftValue.getClass().getName()));
//                }
//            }
            case EQUAL:{
                Object leftValue = left.eval(typeMap, map);
                Object rightValue = right.eval(typeMap, map);
                switch (typeMap.get(left.name)) {
                    case INTEGER -> {
                        yield (Integer) leftValue - (Integer) rightValue == 0;
                    }
                    case FLOAT -> {
                        yield (Float) leftValue - (Float) rightValue == 0;
                    }
                    case VARCHAR -> {
                        yield leftValue.equals(rightValue);
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + typeMap.get(leftValue.getClass().getName()));
                }
            }
            case NULL:{
                yield map.get(name);
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
    }
    Expression left;
    Expression right;
    String name;
    Object value;
    Op op;
}
