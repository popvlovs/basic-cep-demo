package com.hansight.havingsum;

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

import java.util.Properties;

public class HavingSumIB {

    public static void main(String[] args) throws Exception {

        // 内网机器短时间内向外发送大量数据
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-2");
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
                .filter(HavingSumIB::filterByCondition)
                .keyBy(new Tuple2KeySelector("src_address", "dst_address"))
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new HavingSumIB.HavingSumDoubleAggregate("send_byte"), new HavingSumIB.HavingSumProcessWindowFunction())
                .print();

        env.execute("Having Sum Streaming Job");
    }

    private static boolean filterByCondition(ObjectNode node) {
        return ExpressionUtil.equal(node, "event_name", "网络连接")
                && ExpressionUtil.belong(node, "src_address", "内网IP")
                && !ExpressionUtil.belong(node, "src_address", "web服务器地址")
                && !ExpressionUtil.belong(node, "dst_address", "内网IP");
    }

    private static class Tuple2KeySelector implements KeySelector<ObjectNode, Tuple2<String, String>> {
        private String[] fields;

        public Tuple2KeySelector(String... fields) {
            this.fields = fields;
        }

        @Override
        public Tuple2<String, String> getKey(ObjectNode value) throws Exception {
            return (Tuple2<String, String>) ExpressionUtil.getFieldsAsText(value, fields);
        }
    }

    private static class HavingSumDoubleAggregate implements AggregateFunction<ObjectNode, Double, Double> {

        private String sumField;

        public HavingSumDoubleAggregate(String sumField) {
            this.sumField = sumField;
        }

        @Override
        public Double createAccumulator() {
            return 0.0D;
        }

        @Override
        public Double add(ObjectNode value, Double accumulator) {
            return accumulator + ExpressionUtil.getFieldAsValue(value, sumField, 0.0D);
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    private static class HavingSumProcessWindowFunction extends ProcessWindowFunction<Double, Tuple4<String, Long, Long, Double>, Tuple2<String, String>, TimeWindow> {
        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<Double> elements, Collector<Tuple4<String, Long, Long, Double>> out) throws Exception {
            Double count = elements.iterator().next();
            if (count > 0) {
                out.collect(new Tuple4<>(getTupleKeyAsString(key), context.window().getStart(), context.window().getEnd(), count));
            }
        }
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
