package com.hansight.util.condition;

import com.alibaba.fastjson.JSONObject;
import com.hansight.util.ExpressionUtil;

import java.util.Objects;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class BinaryOperatorCondition extends Condition {

    /**
     * required: 左操作数
     */
    private String leftOperand;

    /**
     * required: 操作符
     */
    private ExpressionOperator operator;

    /**
     * optional: 右操作数
     */
    private Object rightOperand;

    public BinaryOperatorCondition(String leftOperand, ExpressionOperator operator, Object rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    @Override
    public boolean eval(JSONObject element) {
        if (Objects.isNull(rightOperand)) {
            return false;
        }
        boolean result;
        switch (operator) {
            case EQUAL:
                result = ExpressionUtil.equal(element, leftOperand, (String) rightOperand);
                break;
            case GT:
                result = ExpressionUtil.gt(element, leftOperand, (Double) rightOperand);
                break;
            case GTE:
                result = ExpressionUtil.gte(element, leftOperand, (Double) rightOperand);
                break;
            case LT:
                result = ExpressionUtil.lt(element, leftOperand, (Double) rightOperand);
                break;
            case LTE:
                result = ExpressionUtil.lte(element, leftOperand, (Double) rightOperand);
                break;
            case LIKE:
                result = ExpressionUtil.like(element, leftOperand, (String) rightOperand);
                break;
            case RLIKE:
                result = ExpressionUtil.rlike(element, leftOperand, (String) rightOperand);
                break;
            case IN:
                result = ExpressionUtil.in(element, leftOperand, (String) rightOperand);
                break;
            case BELONG:
                result = ExpressionUtil.belong(element, leftOperand, (String) rightOperand);
                break;
            case CONTAIN:
                result = ExpressionUtil.contain(element, leftOperand, rightOperand);
                break;
            default:
                throw new IllegalArgumentException("Unsupported binary operator: " + operator);
        }
        return not != result;
    }

    public static BinaryOperatorCondition equal(String leftOperand, Object rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.EQUAL, rightOperand);
    }

    public static BinaryOperatorCondition gt(String leftOperand, Double rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.GT, rightOperand);
    }

    public static BinaryOperatorCondition gte(String leftOperand, Double rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.GTE, rightOperand);
    }

    public static BinaryOperatorCondition lt(String leftOperand, Double rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.LT, rightOperand);
    }

    public static BinaryOperatorCondition lte(String leftOperand, Double rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.LTE, rightOperand);
    }

    public static BinaryOperatorCondition in(String leftOperand, String rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.IN, rightOperand);
    }

    public static BinaryOperatorCondition contain(String leftOperand, String rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.CONTAIN, rightOperand);
    }

    public static BinaryOperatorCondition belong(String leftOperand, String rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.BELONG, rightOperand);
    }

    public static BinaryOperatorCondition like(String leftOperand, String rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.LIKE, rightOperand);
    }

    public static BinaryOperatorCondition rlike(String leftOperand, String rightOperand) {
        return new BinaryOperatorCondition(leftOperand, ExpressionOperator.RLIKE, rightOperand);
    }
}
