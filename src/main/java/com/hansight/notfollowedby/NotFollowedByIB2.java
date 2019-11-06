package com.hansight.notfollowedby;

import com.hansight.util.ExpressionUtil;
import com.hansight.util.OutputUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NotFollowedByIB2 {

    public static void main(String[] args) throws Exception {

        // 内网主机发起异常DNS通信-异常DNS协议报文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-5");
        DataStream<ObjectNode> rawStream = env.addSource(new FlinkKafkaConsumer010<>("hes-sae-group-0", new JSONKeyValueDeserializationSchema(false), properties)
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

        final OutputTag<ObjectNode> streamOnlyAOrB = new OutputTag<ObjectNode>("side-output-A-or-B"){};
        SingleOutputStreamOperator<ObjectNode> streamToSplit =  rawStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode node, Context ctx, Collector<ObjectNode> out) throws Exception {
                out.collect(node);
                // Only A or B
                if (ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && !ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && !ExpressionUtil.equal(node, "event_digest", "nta_flow")
                        || ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6")) {
                    ctx.output(streamOnlyAOrB, node);
                }
            }
        });
        DataStream<ObjectNode> stream = streamToSplit.getSideOutput(streamOnlyAOrB);

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToNext();
        Pattern<ObjectNode, ObjectNode> pattern = Pattern.<ObjectNode>
                begin("sub-pattern-A", skipStrategy)
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode node, Context<ObjectNode> context) throws Exception {

                        // A, 网络连接, 源地址 belong 内网IP and 目的端口 = 53 and not 目的地址 belong 内网IP and 发送流量 > 1000000 and not 目的地址 belong 保留IP地址列表 and 事件摘要 = "nta_flow"
                        return ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && !ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && !ExpressionUtil.equal(node, "event_digest", "nta_flow");
                    }
                })
                .followedBy("sub-pattern-B")
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode node, Context<ObjectNode> ctx) throws Exception {
                        // B, DNS查询, 源地址 belong 内网IP and not 目的地址 belong 内网IP
                        boolean expr = ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6");

                        // A.源地址 = B.源地址 and A.目的地址 = B.目的地址 and A.源端口 = B.源端口 and A.目的端口 = B.目的端口
                        Iterable<ObjectNode> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                        ObjectNode nodeA = prevEvents.iterator().next();
                        boolean relationExpr = ExpressionUtil.equal(nodeA, "src_address", node, "src_address") && ExpressionUtil.equal(nodeA, "dst_address", node, "dst_address") && ExpressionUtil.equal(nodeA, "src_port", node, "src_port") && ExpressionUtil.equal(nodeA, "dst_port", node, "dst_port");

                        return expr && relationExpr;
                    }
                })
                .within(Time.minutes(10));

        final OutputTag<String> outputTag = new OutputTag<String>("not-followed-by-side-output"){};
        SingleOutputStreamOperator output = CEP.pattern(stream, pattern)
                .flatSelect(
                        outputTag,
                        new PatternFlatTimeoutFunction<ObjectNode, String>() {
                            @Override
                            public void timeout(Map<String, List<ObjectNode>> patterns,
                                                long timeoutTimestamp,
                                                Collector<String> collector) throws Exception {
                                collector.collect(OutputUtil.asText(patterns));
                            }
                        },
                        new PatternFlatSelectFunction<ObjectNode, String>() {
                            @Override
                            public void flatSelect(Map<String, List<ObjectNode>> patterns, Collector<String> collector) throws Exception {
                                // do nothing...
                            }
                        })
                .name("Pattern-NotFollowedBy")
                .uid("Pattern-NotFollowedBy");

        output.getSideOutput(outputTag).print();

        env.execute("Not Followed By Streaming Job");
    }
}
