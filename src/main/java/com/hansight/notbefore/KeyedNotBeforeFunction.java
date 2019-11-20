package com.hansight.notbefore;

import com.hansight.util.ExpressionUtil;
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

public class KeyedNotBeforeFunction extends KeyedProcessFunction<String, ObjectNode, List<ObjectNode>> {

    private transient ValueState<Map> lastAState;
    private transient ValueState<Map> silencePeriod;
    private String[] groupFieldsA;
    private String[] groupFieldsB;

    public KeyedNotBeforeFunction(String[] groupFieldsA, String[] groupFieldsB) {
        this.groupFieldsA = groupFieldsA;
        this.groupFieldsB = groupFieldsB;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastAState = getRuntimeContext().getState(new ValueStateDescriptor<>("OperatorState-not-before-last-A", Map.class));
        silencePeriod = getRuntimeContext().getState(new ValueStateDescriptor<>("OperatorState-not-before-silence-period", Map.class));
    }

    @Override
    public void processElement(ObjectNode value, Context ctx, Collector<List<ObjectNode>> out) throws Exception {
        //process(value, ctx, out);
    }

    private void process(ObjectNode node, Context ctx, Collector<List<ObjectNode>> out) throws Exception {
        if (ExpressionUtil.equal(node, "event_name", "邮件登陆")) {
            // On event A
            String group = ExpressionUtil.getGroupSignature(node, groupFieldsA);
            if (noPrevA(group)) {
                updateLastEventState(node);
            } else {
                ObjectNode lastA = getPrevA(group);
                if (ExpressionUtil.getFieldAsTimestamp(node, "occur_time") > ExpressionUtil.getFieldAsTimestamp(lastA, "occur_time")) {
                    updateLastEventState(node);
                }
            }
        } else if (ExpressionUtil.equal(node, "event_name", "邮件发送")) {
            // On event B
            Long occurTimeB = ExpressionUtil.getFieldAsTimestamp(node, "occur_time");
            String group = ExpressionUtil.getGroupSignature(node, groupFieldsB);
            if (noPrevA(group)) {
                output(out, group, occurTimeB, node);
            } else {
                ObjectNode lastA = getPrevA(group);
                if (lastA == null) {
                    output(out, group, occurTimeB, node);
                } else {
                    Long occurTimeA = ExpressionUtil.getFieldAsTimestamp(lastA, "occur_time");
                    if (occurTimeB - occurTimeA > Time.minutes(10).toMilliseconds()) {
                        output(out, group, occurTimeB, node, lastA);
                    }
                }
            }
        }
    }

    private void output(Collector<List<ObjectNode>> out, String groupSignature, Long time, ObjectNode... data) throws Exception {
        // 加入输出的静默周期，防止大量冗余输出
        if (time - getPrevOutputTimestamp(groupSignature) > Time.seconds(5).toMilliseconds()) {
            setPrevOutputTimestamp(groupSignature, time);
            out.collect(Arrays.asList(data));
        }
    }

    private long getPrevOutputTimestamp(String groupSignature) throws Exception {
        Map<String, Long> prevOutputTime = (Map<String, Long>) silencePeriod.value();
        if (prevOutputTime == null) {
            return 0L;
        }
        return prevOutputTime.getOrDefault(groupSignature, 0L);
    }

    synchronized private void setPrevOutputTimestamp(String groupSignature, Long time) throws Exception {
        if (silencePeriod.value() != null) {
            Map<String, Long> outputTimestamp = new ConcurrentHashMap<>();
            outputTimestamp.put(groupSignature, time);
            silencePeriod.update(outputTimestamp);
        } else {
            Map<String, Long> prevOutputTime = (Map<String, Long>) silencePeriod.value();
            if (prevOutputTime != null) {
                prevOutputTime.put(groupSignature, time);
            } else {
                Map<String, Long> outputTimestamp = new ConcurrentHashMap<>();
                outputTimestamp.put(groupSignature, time);
                silencePeriod.update(outputTimestamp);
            }
        }
    }

    private boolean noPrevA(String groupSignature) throws Exception {
        return getPrevA(groupSignature) == null;
    }

    private ObjectNode getPrevA(String groupSignature) throws Exception {
        Map<String, ObjectNode> state = (Map<String, ObjectNode>) lastAState.value();
        if (state == null) {
            return null;
        }
        return state.get(groupSignature);
    }

    synchronized private void updateLastEventState(ObjectNode data) throws Exception {
        String groupSignature = ExpressionUtil.getGroupSignature(data, groupFieldsA);
        if (noPrevA(groupSignature)) {
            Map<String, ObjectNode> state = new ConcurrentHashMap<>();
            state.put(groupSignature, data);
            lastAState.update(state);
        } else {
            Map<String, ObjectNode> state = (Map<String, ObjectNode>) lastAState.value();
            state.put(groupSignature, data);
        }
    }
}
