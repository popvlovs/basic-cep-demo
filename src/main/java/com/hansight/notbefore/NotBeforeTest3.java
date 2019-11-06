package com.hansight.notbefore;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NotBeforeTest3 {

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
        // On A.源地址 = B.源地址 and A.目的地址 = B.目的地址 and A.源端口 = B.源端口 and A.目的端口 = B.目的端口
        String[] groupCondA = new String[] {"dst_address"};
        String[] groupCondB = new String[] {"dst_address"};
        DataStream<List<ObjectNode>> notBefore = stream
                .process(new NotBeforeFunction(groupCondA, groupCondB)).setParallelism(1)
                .uid("Pattern-NotBefore");

        notBefore.print();

        env.execute("Not Before Streaming Job");
    }
}
