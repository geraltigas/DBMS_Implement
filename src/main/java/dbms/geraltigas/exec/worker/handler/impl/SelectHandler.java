package dbms.geraltigas.exec.worker.handler.impl;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.buffer.TableBuffer;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.dataccess.execplan.impl.SelectExec;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exec.worker.handler.Handler;
import dbms.geraltigas.expression.Expression;
import dbms.geraltigas.format.tables.TableDefine;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SelectHandler implements Handler {
    ExecuteEngine executeEngine;
    long threadId = 0;
    @Autowired
    TableBuffer tableBuffer;
    public SelectHandler() {
    }

    @Override
    public void setDataAccesser(ExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public int handle(Statement query) throws ExpressionException, IOException {
        List<Expression> expressions = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Select select = (Select) query;
        PlainSelect selectBody = (PlainSelect)select.getSelectBody();
        List<SelectItem> selectItems = selectBody.getSelectItems();
        List<String> tableNames = new ArrayList<>();
        tableNames.add(selectBody.getFromItem().toString());
        if (selectBody.getJoins() != null) {
            for (Join join : selectBody.getJoins()) {
                tableNames.add(join.getRightItem().toString());
            }
        }
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof AllColumns) {
                for (String tableName : tableNames) {
                    TableDefine tableDefine = tableBuffer.getTableDefine(tableName);
                    names.addAll(tableDefine.getColNames());
                    expressions.addAll(tableDefine.getColNames().stream().map(colName -> new Expression(null,null,colName,null, Expression.Op.NULL)).toList());
                }
                break;
            }
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
            if (selectExpressionItem.getAlias() != null) {
                names.add(selectExpressionItem.getAlias().getName());
                net.sf.jsqlparser.expression.Expression expression = selectExpressionItem.getExpression();
                if (expression instanceof Subtraction subtraction) {
                    Expression expression1 = new Expression();
                    expression1.setLeft(new Expression(null,null,subtraction.getLeftExpression().toString(),null ,Expression.Op.NULL));
                    expression1.setRight(new Expression(null,null,subtraction.getRightExpression().toString(), null, Expression.Op.NULL));
                    expression1.setOp(Expression.Op.SUBTRACT);
                    expressions.add(expression1);
                    continue;
                }
                if (expression instanceof Addition addition) {
                    Expression expression1 = new Expression();
                    expression1.setLeft(new Expression(null,null,addition.getLeftExpression().toString(), null, Expression.Op.NULL));
                    expression1.setRight(new Expression(null,null,addition.getRightExpression().toString(), null, Expression.Op.NULL));
                    expression1.setOp(Expression.Op.PLUS);
                    expressions.add(expression1);
                    continue;
                }
                throw new ExpressionException("expression not supported");
            } else {
                names.add(selectExpressionItem.getExpression().toString());
                expressions.add(new Expression(null,null,selectExpressionItem.getExpression().toString(),null, Expression.Op.NULL));
            }
        }
        net.sf.jsqlparser.expression.Expression whereExpressions = selectBody.getWhere();
        Expression expression = parseExpression(whereExpressions);
        SelectExec execPlan = new SelectExec(expressions,names,tableNames,expression);
        ApplicationContextUtils.autowire(execPlan);
        execPlan.setThreadId(threadId);
        executeEngine.addExecPlan(execPlan);
        return execPlan.hashCode();
    }

    public static Expression parseExpression(net.sf.jsqlparser.expression.Expression expression) throws ExpressionException {
        Expression expression_ = new Expression();
        if (expression == null) {
            return null;
        }
        switch (expression.getClass().getSimpleName()) {
            // bool operations
            case "AndExpression" -> {
                expression_.setLeft(parseExpression(((AndExpression) expression).getLeftExpression()));
                expression_.setRight(parseExpression(((AndExpression) expression).getRightExpression()));
                expression_.setOp(Expression.Op.AND);
                return expression_;
            }
            case "OrExpression" -> {
                expression_.setLeft(parseExpression(((OrExpression) expression).getLeftExpression()));
                expression_.setRight(parseExpression(((OrExpression) expression).getRightExpression()));
                expression_.setOp(Expression.Op.OR);
                return expression_;
            }
            case "EqualsTo" -> {
                expression_.setLeft(parseExpression(((EqualsTo) expression).getLeftExpression()));
                expression_.setRight(parseExpression(((EqualsTo) expression).getRightExpression()));
                expression_.setOp(Expression.Op.EQUAL);
                return expression_;
            }
            //  column value
            case "Column" -> {
                expression_.setLeft(null);
                expression_.setRight(null);
                expression_.setName(((Column) expression).getColumnName());
                expression_.setOp(Expression.Op.NULL);
                return expression_;
            }
            // constant
            case "LongValue" -> {
                expression_.setLeft(null);
                expression_.setRight(null);
                expression_.setName(null);
                expression_.setValue(((LongValue) expression).getValue());
                expression_.setOp(Expression.Op.LONG_VALUE);
                return expression_;
            }
            case "DoubleValue" -> {
                expression_.setLeft(null);
                expression_.setRight(null);
                expression_.setName(null);
                expression_.setValue(((DoubleValue) expression).getValue());
                expression_.setOp(Expression.Op.DOUBLE_VALUE);
                return expression_;
            }
            case "StringValue" -> {
                expression_.setLeft(null);
                expression_.setRight(null);
                expression_.setName(null);
                expression_.setValue(((StringValue) expression).getValue());
                expression_.setOp(Expression.Op.STRING_VALUE);
                return expression_;
            }
            // perentheses
            case "Parenthesis" -> {
                return parseExpression(((Parenthesis) expression).getExpression());
            }
            // disable the support for arithmetic operations
//            case "Subtraction" -> {
//                expression_.setLeft(parseExpression(((Subtraction) expression).getLeftExpression()));
//                expression_.setRight(parseExpression(((Subtraction) expression).getRightExpression()));
//                expression_.setOp(Expression.Op.SUBTRACT);
//                return expression_;
//            }
//            case "Addition" -> {
//                expression_.setLeft(parseExpression(((Addition) expression).getLeftExpression()));
//                expression_.setRight(parseExpression(((Addition) expression).getRightExpression()));
//                expression_.setOp(Expression.Op.PLUS);
//                return expression_;
//            }
            default -> throw new ExpressionException("expression not supported");
        }
    }

    @Override
    public String getResault(int hash) {
        return executeEngine.getResult(hash);
    }
}

