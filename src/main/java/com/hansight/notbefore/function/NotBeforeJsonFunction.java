package com.hansight.notbefore.function;

import com.alibaba.fastjson.JSONObject;
import com.hansight.util.ExpressionUtil;
import com.hansight.util.condition.Condition;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;
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

public class NotBeforeJsonFunction extends ProcessFunction<JSONObject, List<JSONObject>> implements CheckpointedFunction {


    /**
     * Last seen A state
     */
    private transient ListState<Map> lastSeenA;

    /**
     * Silent period state
     */
    private transient ListState<Map> silencePeriod;

    /**
     * Silent period seconds
     */
    private long silentPeriodSecs;

    /**
     * Group by condition
     */
    private String[] groupByConditionA;
    private String[] groupByConditionB;

    /**
     * Event time field, default as eventTimeField
     */
    private String eventTimeField;

    /**
     * Event filter condition
     */
    private Condition conditionA;
    private Condition conditionB;

    public NotBeforeJsonFunction(long silentPeriodSecs, String[] groupByConditionA, String[] groupByConditionB, String eventTimeField, Condition conditionA, Condition conditionB) {
        this.silentPeriodSecs = silentPeriodSecs;
        this.groupByConditionA = groupByConditionA;
        this.groupByConditionB = groupByConditionB;
        this.eventTimeField = eventTimeField;
        this.conditionA = conditionA;
        this.conditionB = conditionB;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Map> stateDescriptor = new ListStateDescriptor<>("OperatorState-not-before-last-A", Map.class);
        lastSeenA = context.getOperatorStateStore().getUnionListState(stateDescriptor);
        ListStateDescriptor<Map> silenceStateDescriptor = new ListStateDescriptor<>("OperatorState-not-before-silence-period", Map.class);
        silencePeriod = context.getOperatorStateStore().getUnionListState(silenceStateDescriptor);
    }

    @Override
    public void processElement(JSONObject node, Context ctx, Collector<List<JSONObject>> out) throws Exception {
        process(node, ctx, out);
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

    private void output(Collector<List<JSONObject>> out, String groupSignature, Long time, JSONObject... data) throws Exception {
        // 加入输出的静默周期，防止大量冗余输出
        if (time - getPrevOutputTimestamp(groupSignature) > Time.seconds(silentPeriodSecs).toMilliseconds()) {
            setPrevOutputTimestamp(groupSignature, time);
            out.collect(Arrays.asList(data));
        }
    }

    private long getPrevOutputTimestamp(String groupSignature) throws Exception {
        if (!silencePeriod.get().iterator().hasNext()) {
            return 0L;
        }
        Map<String, Long> prevOutputTime = (Map<String, Long>) silencePeriod.get().iterator().next();
        if (prevOutputTime == null) {
            return 0L;
        }
        return prevOutputTime.getOrDefault(groupSignature, 0L);
    }

    synchronized private void setPrevOutputTimestamp(String groupSignature, Long time) throws Exception {
        if (!silencePeriod.get().iterator().hasNext()) {
            Map<String, Long> outputTimestamp = new ConcurrentHashMap<>();
            outputTimestamp.put(groupSignature, time);
            silencePeriod.update(Collections.singletonList(outputTimestamp));
        } else {
            Map<String, Long> prevOutputTime = (Map<String, Long>) silencePeriod.get().iterator().next();
            if (prevOutputTime != null) {
                prevOutputTime.put(groupSignature, time);
            } else {
                Map<String, Long> outputTimestamp = new ConcurrentHashMap<>();
                outputTimestamp.put(groupSignature, time);
                silencePeriod.update(Collections.singletonList(outputTimestamp));
            }
        }
    }

    private boolean noPrevA(String groupSignature) throws Exception {
        return getPrevA(groupSignature) == null;
    }

    private JSONObject getPrevA(String groupSignature) throws Exception {
        if (!lastSeenA.get().iterator().hasNext()) {
            return null;
        }
        Map<String, JSONObject> state = (Map<String, JSONObject>) lastSeenA.get().iterator().next();
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
            lastSeenA.update(Collections.singletonList(state));
        } else {
            Map<String, JSONObject> state = lastSeenA.get().iterator().next();
            state.put(groupSignature, data);
        }
    }
}
