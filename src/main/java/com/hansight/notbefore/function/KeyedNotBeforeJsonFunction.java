package com.hansight.notbefore.function;

import com.alibaba.fastjson.JSONObject;
import com.hansight.util.ExpressionUtil;
import com.hansight.util.condition.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/6
 * @description .
 */

public class KeyedNotBeforeJsonFunction extends KeyedProcessFunction<String, JSONObject, List<JSONObject>> {

    /**
     * Last seen A state
     */
    private transient ValueState<Map> lastSeenA;

    /**
     * Silent period state
     */
    private transient ValueState<Map> silentPeriod;

    private long silentPeriodSecs;

    /**
     * Group by condition
     */
    private String[] groupByConditionA;
    private String[] groupByConditionB;

    /**
     * Event time field, default as "occur_time"
     */
    private String eventTimeField;

    /**
     * Event filter condition 
     */
    private Condition conditionA;
    private Condition conditionB;

    public KeyedNotBeforeJsonFunction(long silentPeriodSecs, String[] groupByConditionA, String[] groupByConditionB, String eventTimeField, Condition conditionA, Condition conditionB) {
        this.silentPeriodSecs = silentPeriodSecs;
        this.groupByConditionA = groupByConditionA;
        this.groupByConditionB = groupByConditionB;
        this.eventTimeField = eventTimeField;
        this.conditionA = conditionA;
        this.conditionB = conditionB;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastSeenA = getRuntimeContext().getState(new ValueStateDescriptor<>("OperatorState-not-before-last-A", Map.class));
        silentPeriod = getRuntimeContext().getState(new ValueStateDescriptor<>("OperatorState-not-before-silence-period", Map.class));
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<List<JSONObject>> out) throws Exception {
        process(value, ctx, out);
    }

    private void process(JSONObject node, Context ctx, Collector<List<JSONObject>> out) throws Exception {
        if (conditionA.eval(node)) {
            // On event A
            String group = ExpressionUtil.getGroupSignature(node, groupByConditionA);
            if (noPrevA(group)) {
                updateLastEventState(node);
            } else {
                JSONObject lastSeenA = getPrevA(group);
                if (ExpressionUtil.getFieldAsTimestamp(node, eventTimeField) > ExpressionUtil.getFieldAsTimestamp(lastSeenA, eventTimeField)) {
                    updateLastEventState(node);
                }
            }
        } else if (conditionB.eval(node)) {
            // On event B
            Long occurTimeB = ExpressionUtil.getFieldAsTimestamp(node, eventTimeField);
            String group = ExpressionUtil.getGroupSignature(node, groupByConditionB);
            if (noPrevA(group)) {
                output(out, group, occurTimeB, node);
            } else {
                JSONObject lastSeenA = getPrevA(group);
                if (lastSeenA == null) {
                    output(out, group, occurTimeB, node);
                } else {
                    Long occurTimeA = ExpressionUtil.getFieldAsTimestamp(lastSeenA, eventTimeField);
                    if (occurTimeB - occurTimeA > Time.minutes(10).toMilliseconds()) {
                        output(out, group, occurTimeB, node, lastSeenA);
                    }
                }
            }
        }
    }

    public boolean isInterestedIn(JSONObject node) {
        return conditionA.eval(node) || conditionB.eval(node);
    }

    public String extractGroupSignature(JSONObject node) {
        if (conditionA.eval(node)) {
            return ExpressionUtil.getGroupSignature(node, groupByConditionA);
        } else if (conditionB.eval(node)) {
            return ExpressionUtil.getGroupSignature(node, groupByConditionB);
        } else {
            return "None";
        }
    }

    private void output(Collector<List<JSONObject>> out, String groupSignature, Long time, JSONObject... data) throws Exception {
        // Output with silent period
        if (time - getPrevOutputTimestamp(groupSignature) > Time.seconds(silentPeriodSecs).toMilliseconds()) {
            setPrevOutputTimestamp(groupSignature, time);
            out.collect(Arrays.asList(data));
        }
    }

    private long getPrevOutputTimestamp(String groupSignature) throws Exception {
        Map<String, Long> prevOutputTime = (Map<String, Long>) silentPeriod.value();
        if (prevOutputTime == null) {
            return 0L;
        }
        return prevOutputTime.getOrDefault(groupSignature, 0L);
    }

    synchronized private void setPrevOutputTimestamp(String groupSignature, Long time) throws Exception {
        if (silentPeriod.value() != null) {
            Map<String, Long> outputTimestamp = new ConcurrentHashMap<>();
            outputTimestamp.put(groupSignature, time);
            silentPeriod.update(outputTimestamp);
        } else {
            Map<String, Long> prevOutputTime = (Map<String, Long>) silentPeriod.value();
            if (prevOutputTime != null) {
                prevOutputTime.put(groupSignature, time);
            } else {
                Map<String, Long> outputTimestamp = new ConcurrentHashMap<>();
                outputTimestamp.put(groupSignature, time);
                silentPeriod.update(outputTimestamp);
            }
        }
    }

    private boolean noPrevA(String groupSignature) throws Exception {
        return getPrevA(groupSignature) == null;
    }

    private JSONObject getPrevA(String groupSignature) throws Exception {
        Map<String, JSONObject> state = (Map<String, JSONObject>) lastSeenA.value();
        if (state == null) {
            return null;
        }
        return state.get(groupSignature);
    }

    synchronized private void updateLastEventState(JSONObject data) throws Exception {
        String groupSignature = ExpressionUtil.getGroupSignature(data, groupByConditionA);
        if (noPrevA(groupSignature)) {
            Map<String, JSONObject> state = new ConcurrentHashMap<>();
            state.put(groupSignature, data);
            lastSeenA.update(state);
        } else {
            Map<String, JSONObject> state = (Map<String, JSONObject>) lastSeenA.value();
            state.put(groupSignature, data);
        }
    }
}
