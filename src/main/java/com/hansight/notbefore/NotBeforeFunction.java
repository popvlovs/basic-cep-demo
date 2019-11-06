package com.hansight.notbefore;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
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

public class NotBeforeFunction extends ProcessFunction<ObjectNode, List<ObjectNode>> implements CheckpointedFunction {

    private transient ListState<Map> lastAState;
    private transient ListState<Map> silencePeriod;
    private String[] groupFieldsA;
    private String[] groupFieldsB;

    public NotBeforeFunction(String[] groupFieldsA, String[] groupFieldsB) {
        this.groupFieldsA = groupFieldsA;
        this.groupFieldsB = groupFieldsB;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Map> stateDescriptor = new ListStateDescriptor<>("OperatorState-not-before-last-A", Map.class);
        lastAState = context.getOperatorStateStore().getUnionListState(stateDescriptor);
        ListStateDescriptor<Map> silenceStateDescriptor = new ListStateDescriptor<>("OperatorState-not-before-silence-period", Map.class);
        silencePeriod = context.getOperatorStateStore().getUnionListState(silenceStateDescriptor);
    }

    @Override
    public void processElement(ObjectNode node, Context ctx, Collector<List<ObjectNode>> out) throws Exception {
        process(node, ctx, out);
    }

    private void process(ObjectNode node, Context ctx, Collector<List<ObjectNode>> out) throws Exception {
        // todo 加入watermark机制
        // 加入输出的静默周期，防止大量冗余输出
        if (ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6")) {
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
        } else if (ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && !ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && !ExpressionUtil.equal(node, "event_digest", "nta_flow")) {
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
        if (time - getPrevOutputTimestamp(groupSignature) > Time.seconds(5).toMilliseconds()) {
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

    private ObjectNode getPrevA(String groupSignature) throws Exception {
        if (!lastAState.get().iterator().hasNext()) {
            return null;
        }
        Map<String, ObjectNode> state = (Map<String, ObjectNode>) lastAState.get().iterator().next();
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
            lastAState.update(Collections.singletonList(state));
        } else {
            Map<String, ObjectNode> state = lastAState.get().iterator().next();
            state.put(groupSignature, data);
        }
    }
}
