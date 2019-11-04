package com.hansight.havingany;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class HavingAnyIB {

    public static void main(String[] args) throws Exception {

        // 内部邮件服务器向黑名单IP发送邮件
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-3");
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
                .filter(HavingAnyIB::filterByCondition)
                .print();

        env.execute("Having Any Streaming Job");
    }

    private static boolean filterByCondition(ObjectNode node) {
        return ExpressionUtil.equal(node, "event_name", "网络连接")
                && ExpressionUtil.belong(node, "src_address", "邮件服务器地址")
                && ExpressionUtil.belong(node, "dst_address", "黑名单IP")
                && (ExpressionUtil.equal(node, "app_protocol", "smtp") || ExpressionUtil.equal(node, "dst_port", "25"))
                && !ExpressionUtil.belong(node, "dst_address", "保留IP地址列表");
    }
}
