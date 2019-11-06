package com.hansight.notbefore;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NotBeforeTest2 {

    public static void main(String[] args) throws Exception {

        // 内网主机发起异常DNS通信-异常DNS协议报文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-5");
        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>("hes-sae-group-0", new JSONKeyValueDeserializationSchema(false), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                        JsonNode timestamp = element.findValue("occur_time");
                        if (timestamp == null) {
                            return System.currentTimeMillis();
                        } else {
                            return timestamp.asLong(0);
                        }
                    }
                })
                .setStartFromEarliest())
                .name("Kafka-source")
                .uid("kafka-source");

        // 虚构这样一条规则
        // 内网主机发起异常DNS通信-异常DNS协议报文
        // not A before B
        // A: DNS查询，源地址 belong 内网IP and not 目的地址 belong 内网IP
        // B: 网络连接，源地址 belong 内网IP and 目的端口 = 53 and not 目的地址 belong 内网IP and 发送流量 > 1000000 and not 目的地址 belong 保留IP地址列表 and 事件摘要 = "nta_flow"
        // On A.源地址 = B.源地址 and A.目的地址 = B.目的地址 and A.源端口 = B.源端口 and A.目的端口 = B.目的端口
        String[] groupCondA = new String[] {"dst_address"};
        String[] groupCondB = new String[] {"dst_address"};
        DataStream<List<ObjectNode>> notBefore = stream
                .process(new NotBeforeFunction(groupCondA, groupCondB))
                .uid("Pattern-NotBefore");

        notBefore.print();

        env.execute("Not Before Streaming Job");
    }

    private static class NotBeforeFunction extends ProcessFunction<ObjectNode, List<ObjectNode>> implements CheckpointedFunction {

        private transient ListState<Map> lastAState;
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
        }

        @Override
        public void processElement(ObjectNode node, Context ctx, Collector<List<ObjectNode>> out) throws Exception {
            // todo 加入watermark机制
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
                    out.collect(Collections.singletonList(node));
                } else {
                    ObjectNode lastA = getPrevA(group);
                    if (lastA == null) {
                        out.collect(Collections.singletonList(node));
                    } else {
                        Long occurTimeA = ExpressionUtil.getFieldAsTimestamp(lastA, "occur_time");
                        if (occurTimeB - occurTimeA > Time.minutes(10).toMilliseconds()) {
                            out.collect(Arrays.asList(node, lastA));
                        }
                    }
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
}
