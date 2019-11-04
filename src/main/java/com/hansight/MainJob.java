package com.hansight;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class MainJob {

    public static void main(String[] args) throws Exception {

        // 内部事件-主机特定端口被大量扫描
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-1");
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
                .filter(MainJob::filterByCondition)
                .keyBy(new Tuple2KeySelector("dst_address", "dst_port"))
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .aggregate(new MainJob.HavingCountDistinctAggregate(), new MainJob.HavingCountProcessWindowFunction())
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

    private static class Tuple2KeySelector implements KeySelector<ObjectNode, Tuple2<String, String>> {
        private String[] fields;

        public Tuple2KeySelector(String... fields) {
            this.fields = fields;
        }

        @Override
        public Tuple2<String, String> getKey(ObjectNode value) throws Exception {
            return (Tuple2<String, String>) getFieldsAsText(value, fields);
        }
    }

    private static class HavingCountDistinctAggregate implements AggregateFunction<ObjectNode, Set<String>, Long> {
        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> add(ObjectNode value, Set<String> accumulator) {
            accumulator.add(getFieldAsText(value, "src_address"));
            return accumulator;
        }

        @Override
        public Long getResult(Set<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            a.addAll(b);
            return a;
        }
    }

    private static class HavingCountProcessWindowFunction extends ProcessWindowFunction<Long, Tuple4<String, Long, Long, Long>, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> key, Context context, java.lang.Iterable<Long> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
            Long count = elements.iterator().next();
            out.collect(new Tuple4<>(getTupleKeyAsString(key), context.window().getStart(), context.window().getEnd(), count));
        }
    }

    private static Tuple getFieldsAsText(ObjectNode val, String... fields) {
        int fieldNum = fields.length;
        Tuple tuple = Tuple.newInstance(fieldNum);
        for (int i = 0; i < fields.length; i++) {
            tuple.setField(getFieldAsText(val, fields[i]), i);
        }
        return tuple;
    }

    private static String getFieldAsText(ObjectNode val, String field) {
        JsonNode nodeVal = val.findValue(field);
        if (nodeVal == null) {
            return "None";
        }
        return nodeVal.asText("None");
    }

    private static String getTupleKeyAsString(Tuple key) {
        StringBuilder sb = new StringBuilder();
        int arity = key.getArity();
        for (int i = 0; i < arity; ++i) {
            String fieldVal = key.getField(i);
            sb.append(fieldVal == null ? "None" : fieldVal);
            if (i < arity - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
}
