package com.hansight.notbefore;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
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

import java.util.Map;
import java.util.Properties;

public class NotBeforeTest {

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
        DataStream<ObjectNode> notBefore = stream
                .process(new ProcessFunction<ObjectNode, ObjectNode>() {
                    private ValueState<ObjectNode> lastA;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastA = getRuntimeContext().getState(new ValueStateDescriptor<>("lastA", ObjectNode.class));
                    }

                    @Override
                    public void processElement(ObjectNode node, Context ctx, Collector<ObjectNode> out) throws Exception {
                        if (ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6")) {
                            // On event A
                            lastA.update(node);
                        } else if (ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && !ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && !ExpressionUtil.equal(node, "event_digest", "nta_flow")) {
                            // On event B
                            Long occurTimeB = ExpressionUtil.getFieldAsTimestamp(node, "occur_time");
                            if (lastA.value() != null) {
                                Long occurTimeA = ExpressionUtil.getFieldAsTimestamp(lastA.value(), "occur_time");
                                if (occurTimeB - occurTimeA > Time.seconds(10).toMilliseconds()) {
                                    out.collect(node);
                                }
                            }
                        }
                        // todo 1 如何支持watermark？需要把部分数据缓存到有序队列中，直到watermark到达，再把数据出列，达成部分正序的效果
                    }
                })
                .uid("Pattern-NotBefore");

        notBefore.print();

        env.execute("Not Before Streaming Job");
    }

    private static class NotBeforeFunction extends ProcessFunction<ObjectNode, ObjectNode> implements CheckpointedFunction {

        //private transient ListState<Map<String, ObjectNode>> checkpointedState;
        private transient ValueState<ObjectNode> lastA;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            /*ListStateDescriptor<ObjectNode> stateDescriptor = new ListStateDescriptor<>("OperatorState-not-before-last-A", ObjectNode.class);
            checkpointedState = context.getOperatorStateStore().getUnionListState(stateDescriptor);
            if (context.isRestored()) {
                for ()
            }*/
            ValueStateDescriptor<ObjectNode> stateDescriptor = new ValueStateDescriptor<ObjectNode>("OperatorState-not-before-last-A", ObjectNode.class);
            lastA = context.getKeyedStateStore().getState(stateDescriptor);
        }

        @Override
        public void processElement(ObjectNode node, Context ctx, Collector<ObjectNode> out) throws Exception {
            if (ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6")) {
                // On event A
                lastA.update(node);
            } else if (ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && !ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && !ExpressionUtil.equal(node, "event_digest", "nta_flow")) {
                // On event B
                Long occurTimeB = ExpressionUtil.getFieldAsTimestamp(node, "occur_time");
                if (lastA.value() != null) {
                    Long occurTimeA = ExpressionUtil.getFieldAsTimestamp(lastA.value(), "occur_time");
                    if (occurTimeB - occurTimeA > Time.seconds(10).toMilliseconds()) {
                        out.collect(node);
                    }
                }
            }
        }
    }
}
