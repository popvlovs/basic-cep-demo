package com.hansight.havingcount;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class HavingCountIB {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-0");
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

        stream
                .filter(HavingCountIB::filterByCondition)
                .keyBy(node -> getFieldAsText(node, "src_address"))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .aggregate(new HavingCountAggregate(), new HavingCountProcessWindowFunction())
                .print();

        env.execute("Followed By Streaming Job");
    }

    private static boolean filterByCondition(ObjectNode node) {
        boolean b1 = ExpressionUtil.equal(node, "event_digest", "nta_alert");
        boolean b2 = !ExpressionUtil.belong(node, "threat_rule_id", "流量检测规则组集合");
        boolean b3 = ExpressionUtil.rlike(node, "threat_info", "EXPLOIT.*");
        boolean b4 = ExpressionUtil.belong(node, "src_address", "内网IP");
        return b1 && b2 && b3 && b4;
    }

    private static class HavingCountAggregate implements AggregateFunction<ObjectNode, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ObjectNode value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class HavingCountProcessWindowFunction extends ProcessWindowFunction<Long, Tuple4<String, Long, Long, Long>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, java.lang.Iterable<Long> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
            Long count = elements.iterator().next();
            out.collect(new Tuple4<>(key, context.window().getStart(), context.window().getEnd(), count));
        }
    }

    private static String getFieldAsText(ObjectNode val, String field) {
        JsonNode nodeVal = val.findValue(field);
        if (nodeVal == null) {
            return "None";
        }
        return nodeVal.asText("None");
    }
}
