package com.hansight.notbefore.function;

import com.hansight.util.condition.Condition;
import com.hansight.util.condition.ConditionUtil;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/26
 * @description .
 */
public class KeyedNotBeforeFunctionBuilder {
    /**
     * A事件分组条件
     * Assertion: groupByConditionA.len == groupByConditionB.len
     */
    private String[] groupByConditionA = new String[]{};

    /**
     * B事件分组条件
     * Assertion: groupByConditionA.len == groupByConditionB.len
     */
    private String[] groupByConditionB = new String[]{};

    /**
     * 事件时间属性字段，默认为occur_time
     */
    private String eventTimeField = "occur_time";

    /**
     * A事件过滤条件
     */
    private Condition conditionA;

    /**
     * B事件过滤条件
     */
    private Condition conditionB;

    private KeyedNotBeforeFunctionBuilder() {
    }

    public static KeyedNotBeforeFunctionBuilder builder() {
        return new KeyedNotBeforeFunctionBuilder();
    }

    public KeyedNotBeforeFunctionBuilder filterByA(String filter) {
        conditionA = ConditionUtil.fromFilter(filter);
        return this;
    }

    public KeyedNotBeforeFunctionBuilder filterByB(String filter) {
        conditionB = ConditionUtil.fromFilter(filter);
        return this;
    }

    public KeyedNotBeforeFunctionBuilder groupByA(String... groupBy) {
        groupByConditionA = groupBy;
        return this;
    }

    public KeyedNotBeforeFunctionBuilder groupByB(String... groupBy) {
        groupByConditionA = groupBy;
        return this;
    }

    public KeyedNotBeforeFunctionBuilder eventTime(String field) {
        eventTimeField = field;
        return this;
    }

    public KeyedNotBeforeJsonFunction build() {
        return new KeyedNotBeforeJsonFunction(5, groupByConditionA, groupByConditionB, eventTimeField, conditionA, conditionB);
    }
}
