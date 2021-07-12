package com.hansight.util.condition;

import com.hansight.atom.panther.parser.bean.HaliteSubExpression;
import com.hansight.util.HaliteTranslator;

import java.util.Stack;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class ConditionUtil {

    public static Condition fromFilter(String filter) {
        Stack<Object> expressions = HaliteTranslator.getPostfixExpressions(filter);

        Stack<Condition> stack = new Stack<>();

        for (Object item : expressions) {
            if (item == null) {
                throw new IllegalArgumentException("Invalid halite expression: " + filter);
            }
            if (item instanceof HaliteSubExpression) {
                HaliteSubExpression subExpression = (HaliteSubExpression) item;
                stack.push(toCondition(subExpression));
            } else if (item instanceof String) {
                switch (((String) item).toLowerCase().trim()) {
                    case "and":
                        stack.push(AndConditionGroup.of(stack.pop(), stack.pop()));
                        break;
                    case "or":
                        stack.push(OrConditionGroup.of(stack.pop(), stack.pop()));
                        break;
                    case "not":
                        stack.push(stack.pop().reverse());
                        break;
                    case "(":
                    case ")":
                        throw new IllegalArgumentException("Invalid prefix expression: '(',')'");
                    default:
                        throw new IllegalArgumentException("Unsupported halite: " + filter + ", " + item);
                }
            } else {
                throw new IllegalArgumentException("Unsupported halite: " + filter + ", " + item);
            }
        }
        return stack.pop();
    }

    private static Condition toCondition(HaliteSubExpression expression) {
        switch (expression.getOperator().toLowerCase()) {
            case "=":
                return BinaryOperatorCondition.equal(expression.getField(), expression.getValue());
            case "!=":
                return BinaryOperatorCondition.equal(expression.getField(), expression.getValue()).reverse();
            case "like":
                return BinaryOperatorCondition.like(expression.getField(), expression.getValue());
            case "rlike":
                return BinaryOperatorCondition.rlike(expression.getField(), expression.getValue());
            case ">":
                return BinaryOperatorCondition.gt(expression.getField(), Double.parseDouble(expression.getValue()));
            case ">=":
                return BinaryOperatorCondition.gte(expression.getField(), Double.parseDouble(expression.getValue()));
            case "<":
                return BinaryOperatorCondition.lt(expression.getField(), Double.parseDouble(expression.getValue()));
            case "<=":
                return BinaryOperatorCondition.lte(expression.getField(), Double.parseDouble(expression.getValue()));
            case "exist":
                return UnaryOperatorCondition.exist(expression.getField());
            case "not_exist":
                return UnaryOperatorCondition.exist(expression.getField()).reverse();
            case "in":
                return BinaryOperatorCondition.in(expression.getField(), expression.getValue());
            case "belong":
                return BinaryOperatorCondition.belong(expression.getField(), expression.getValue());
            case "contain":
                return BinaryOperatorCondition.contain(expression.getField(), expression.getValue());
            case "match":
                return UnaryOperatorCondition.match(expression.getField());
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }
}
