package com.hansight.util.condition;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public abstract class AbstractCondition<T> {

    abstract public boolean eval(T element);

    public enum ExpressionOperator {
        BELONG, EQUAL, GT, GTE, LT, LTE, MATCH, EXIST, CONTAIN, IN, LIKE, RLIKE
    }
}
