package com.hansight.util.condition;

import com.alibaba.fastjson.JSONObject;
import com.hansight.util.ExpressionUtil;

import java.util.Objects;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class UnaryOperatorCondition extends Condition {

    /**
     * required: 左操作数
     */
    private String leftOperand;

    /**
     * required: 操作符
     */
    private ExpressionOperator operator;

    public UnaryOperatorCondition(String leftOperand, ExpressionOperator operator) {
        this.leftOperand = leftOperand;
        this.operator = operator;
    }

    public boolean eval(JSONObject element) {
        switch (operator) {
            case MATCH:
                return not != ExpressionUtil.match(element, leftOperand);
            case EXIST:
                return not != ExpressionUtil.exist(element, leftOperand);
            default:
                throw new IllegalArgumentException("Unsupported unary operator: " + operator);
        }
    }

    public static UnaryOperatorCondition exist(String leftOperand) {
        return new UnaryOperatorCondition(leftOperand, ExpressionOperator.EXIST);
    }

    public static UnaryOperatorCondition match(String leftOperand) {
        return new UnaryOperatorCondition(leftOperand, ExpressionOperator.MATCH);
    }
}
