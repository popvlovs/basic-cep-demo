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
public class NotBeforeFunctionBuilder {
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

    private NotBeforeFunctionBuilder() {
    }

    public static NotBeforeFunctionBuilder builder() {
        return new NotBeforeFunctionBuilder();
    }

    public NotBeforeFunctionBuilder filterByA(String filter) {
        conditionA = ConditionUtil.fromFilter(filter);
        return this;
    }

    public NotBeforeFunctionBuilder filterByB(String filter) {
        conditionB = ConditionUtil.fromFilter(filter);
        return this;
    }

    public NotBeforeFunctionBuilder groupByA(String... groupBy) {
        groupByConditionA = groupBy;
        return this;
    }

    public NotBeforeFunctionBuilder groupByB(String... groupBy) {
        groupByConditionA = groupBy;
        return this;
    }

    public NotBeforeFunctionBuilder eventTime(String field) {
        eventTimeField = field;
        return this;
    }

    public NotBeforeJsonFunction build() {
        return new NotBeforeJsonFunction(5, groupByConditionA, groupByConditionB, eventTimeField, conditionA, conditionB);
    }
}
