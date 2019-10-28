package com.hansight;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FollowedBySteamingJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-0");
        List<String> topics = Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .map(idx -> String.format("hes-sae-group-%d", idx))
                .collect(Collectors.toList());
        int maxOutOfOrderness = 3000;
        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(topics, new JSONKeyValueDeserializationSchema(false), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(maxOutOfOrderness)) {
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


        // Start
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        // 内网主机遭受远程漏洞攻击后创建操作系统账号
        Pattern<ObjectNode, ObjectNode> pattern = Pattern.<ObjectNode>
                begin("sub-pattern-A", skipStrategy).where(new IterativeCondition<ObjectNode>() {
                        @Override
                        public boolean filter(ObjectNode val, Context<ObjectNode> context) throws Exception {
                            // 全局事件A：目的地址 belong 内网IP and (事件名称 = "远程漏洞攻击" or (事件摘要 = "nta_alert" and 事件名称 like "漏洞"))
                            boolean expr =  ExpressionUtil.belong(val, "dst_address", "内网IP") && (ExpressionUtil.equal(val, "event_name", "远程漏洞攻击") || (ExpressionUtil.equal(val, "event_digest", "nta_alert") && ExpressionUtil.like(val, "event_name", "漏洞")));
                            return expr;
                        }
                })
                .next("sub-pattern-B").where(new IterativeCondition<ObjectNode>() {
                        @Override
                        public boolean filter(ObjectNode val, Context<ObjectNode> ctx) throws Exception {
                            // 全局事件B：(Windows事件ID = 4720 or (事件名称 = "账号创建" and 数据源 like "Linux")) and 目的地址 belong 内网IP
                            boolean expr = (ExpressionUtil.equal(val, "windows_event_id", 4720) || (ExpressionUtil.equal(val, "event_name", "账号创建") && ExpressionUtil.like(val, "data_source", "Linux"))) && ExpressionUtil.belong(val, "dst_address", "内网IP");
                            Iterable<ObjectNode> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                            ObjectNode eventA = prevEvents.iterator().next();
                            boolean relationExpr = StringUtils.equals(eventA.findValue("src_address").asText(), val.findValue("dst_address").asText());
                            return expr && relationExpr;
                        }
                })
                .within(Time.seconds(10));

        DataStream<ObjectNode> input = stream.keyBy(data -> data.findValue("username"));
        SingleOutputStreamOperator output = CEP.pattern(input, pattern)
                .select(matchedEvents -> {
                    StringBuilder sb = new StringBuilder();
                    for (String patternName : matchedEvents.keySet()) {
                        sb.append(patternName + ": ");
                        List<ObjectNode> events = matchedEvents.get(patternName);
                        String vals = events.stream().map(ObjectNode::toString).reduce((a, b) -> a + ',' + b).orElse("");
                        sb.append("[" + vals + "] ");
                    }
                    return sb.toString();
                })
                .name("Pattern-RepeatUntil")
                .uid("Pattern-RepeatUntil");

        // End
        FlinkKafkaProducer010<String> kafkaSink = new FlinkKafkaProducer010<>(
                "172.16.100.146:9092",
                "alarm_consecutive_login",
                new SimpleStringSchema()
        );
        kafkaSink.setWriteTimestampToKafka(true);

        env.execute("Followed By Streaming Job");
    }
}
