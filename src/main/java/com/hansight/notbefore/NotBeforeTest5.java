package com.hansight.notbefore;

import com.hansight.DiscardingSink;
import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

public class NotBeforeTest5 {

    public static void main(String[] args) throws Exception {

        // 内网主机发起异常DNS通信-异常DNS协议报文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 开启OperatorChain数据重用
        env.getConfig().enableObjectReuse();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "testGroup");
        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>("hes-sae-group-0", new JSONKeyValueDeserializationSchema(false), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                        return ExpressionUtil.getFieldAsLong(element, "row_time", 0L);
                    }
                })
                .setStartFromEarliest())
                .name("Kafka-source")
                .uid("kafka-source");

        DataStream<List<ObjectNode>> notBefore = stream
                .keyBy(val -> ExpressionUtil.getFieldAsText(val, "src_address", "None"))
                .process(new KeyedNotBeforeFunction(new String[]{}, new String[]{}))
                .uid("Process Function Back-pressure Test")
                .name("Process Function Back-pressure Test");

        notBefore.addSink(new DiscardingSink<>());

        env.execute("Not Before Streaming Job");
    }
}
