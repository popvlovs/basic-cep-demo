package com.hansight;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/8
 * @description .
 */
public class StreamingJobTest1 {

    public static void main(String[] args) throws Exception {
        getEnv().execute("HanSight CEP Demo A1");
    }

    public static StreamExecutionEnvironment getEnv() {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.106.67:9092");
        properties.setProperty("group.id", "consumer-group-A1");
        // 最大可接受的乱序时间范围
        long maxTolerableDelayMillis = 3000;
        DataStream<Map> stream = env.addSource(new FlinkKafkaConsumer010<>("userActionsV3", new DeserializationSchema<Map>() {
            @Override
            public Map deserialize(byte[] message) throws IOException {
                String data = new java.lang.String(message);
                return JSONObject.parseObject(data);
            }

            @Override
            public boolean isEndOfStream(Map nextElement) {
                return false;
            }

            @Override
            public TypeInformation<Map> getProducedType() {
                return TypeExtractor.getForClass(Map.class);
            }
        }, properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Map>(Time.milliseconds(maxTolerableDelayMillis)) {
                    @Override
                    public long extractTimestamp(Map element) {
                        String timestamp = ((JSONObject) element).getString("eventTime");
                        return parseTime(timestamp);
                    }
                })
                .setStartFromEarliest());

        FlinkKafkaProducer010<String> kafkaSink = new FlinkKafkaProducer010<String>(
                "172.16.106.67:9092",
                "result-topic-A1",
                new SimpleStringSchema()
        );
        stream
                .keyBy(item -> item.get("username"))
                .filter(item -> Objects.equals(item.get("action"), "NONE"))
                .map(JSONObject::toJSONString)
                .addSink(kafkaSink)
                .name("kafkaSink");
        return env;
    }

    private static long parseTime(String timestamp) {
        ZonedDateTime zdt = ZonedDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"));
        return zdt.toInstant().toEpochMilli();
    }

    /**
     * javac -encoding UTF-8 -cp "C:\projects\basic-cep-demo\lib\fastjson-1.2.59.jar;C:\projects\basic-cep-demo\lib\flink-cep_2.11-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-java-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-streaming-java_2.11-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-core-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-connector-kafka-0.9_2.11-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-connector-kafka-0.10_2.11-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-connector-kafka-base_2.11-1.9.0.jar;C:\projects\basic-cep-demo\lib\flink-runtime_2.11-1.9.0.jar" src/main/java/com/hansight/StreamingJobTest1.java
     */
}
