package com.hansight.notbefore;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.FastJsonSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class NotBeforeFastJsonReduceTest {

    public static void main(String[] args) throws Exception {

        // 内网主机发起异常DNS通信-异常DNS协议报文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "testGroup");
        DataStream<JSONObject> stream = env.addSource(new FlinkKafkaConsumer010<>("hes-sae-group-0", new FastJsonSchema(), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(JSONObject element) {
                        return element.getLongValue("occur_time");
                    }
                })
                .setStartFromEarliest())
                .name("Kafka-source")
                .uid("kafka-source");

        stream
                .map(NotBeforeFastJsonReduceTest::reduceJson)
                .keyBy(val -> getKey(val, "src_address", "None"))
                .addSink(new DiscardingSink<>());

        env.execute("Not Before Streaming Job");
    }

    private static JSONObject reduceJson(JSONObject element) {
        JSONObject newElement = new JSONObject();
        newElement.put("event_name", element.getString("event_name"));
        newElement.put("src_address", element.getString("src_address"));
        newElement.put("dst_address", element.getString("dst_address"));
        newElement.put("event_digest", element.getString("event_digest"));
        newElement.put("occur_time", element.getString("occur_time"));
        return newElement;
    }

    private static String getKey(JSONObject element, String field, String defaultVal) {
        String val = element.getString(field);
        if (val != null) {
            return val;
        }
        return defaultVal;
    }
}
