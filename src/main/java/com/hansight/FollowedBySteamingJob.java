package com.hansight;

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
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class FollowedBySteamingJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.146:9092");
        properties.setProperty("group.id", "test-consumer-group");
        String topic = "userActionsV3";
        int maxOutOfOrderness = 3000;
        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(topic, new JSONKeyValueDeserializationSchema(false), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(maxOutOfOrderness)) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                        JsonNode timestamp = element.findValue("eventTime");
                        if (timestamp == null) {
                            return System.currentTimeMillis();
                        } else {
                            ZonedDateTime zdt = ZonedDateTime.parse(timestamp.asText(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"));
                            return zdt.toInstant().toEpochMilli();
                        }
                    }
                })
                .setStartFromEarliest())
                .uid("kafka-source");



        // Start

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<ObjectNode, ObjectNode> pattern = Pattern.<ObjectNode>
                begin("sub-pattern-A", skipStrategy).where(new IterativeCondition<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode val, Context<ObjectNode> context) throws Exception {
                return Objects.equals(val.findValue("action").asText(), "Login");
            }
        })
                .followedBy("sub-pattern-B").where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode val, Context<ObjectNode> ctx) throws Exception {
                        Iterable<ObjectNode> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                        if (prevEvents.iterator().hasNext()) {
                            return Objects.equals(val.findValue("action").asText(), "Logout")
                                    && Objects.equals(prevEvents.iterator().next().findValue("region"), val.findValue("region"));
                        }
                        return false;
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
                .uid("Pattern-RepeatUntil");



        // End
        output.print();

        env.execute("Followed By Streaming Job");
    }
}
