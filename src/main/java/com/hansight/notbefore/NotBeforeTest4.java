package com.hansight.notbefore;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

public class NotBeforeTest4 {

    public static void main(String[] args) throws Exception {

        // 内网主机发起异常DNS通信-异常DNS协议报文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "testGroup");
        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>("hes-sae-group-0", new JSONKeyValueDeserializationSchema(false), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                        return ExpressionUtil.getFieldAsLong(element, "occur_time", 0L);
                    }
                })
                .setStartFromEarliest())
                .name("Kafka-source")
                .uid("kafka-source");

        DataStream<List<ObjectNode>> notBefore = stream
                .keyBy(node -> ExpressionUtil.getFieldAsText(node, "src_address", "None"))
                .process(new KeyedProcessFunction<String, ObjectNode, List<ObjectNode>>() {
                    @Override
                    public void processElement(ObjectNode value, Context ctx, Collector<List<ObjectNode>> out) throws Exception {
                        // do nothing
                        // 一个空的process function依然会导致0.6-0.8的反压
                        // 只能怀疑反压是由于network transfer导致的
                    }
                })
                .uid("Process Function Back-pressure Test");

        notBefore.print();

        env.execute("Not Before Streaming Job");
    }
}
