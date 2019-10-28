package com.hansight;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class MainJob {

    public static void main(String[] args) throws Exception {
        // Init streaming env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(Time.seconds(1).toMilliseconds());

        // Timestamp and watermark assigner
        AssignerWithPeriodicWatermarks<ObjectNode> watermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(3000)) {
            @Override
            public long extractTimestamp(ObjectNode element) {
                JsonNode timestamp = element.findValue("occur_time");
                if (timestamp == null) {
                    return System.currentTimeMillis();
                } else {
                    return element.findValue("occur_time").asLong(0L);
                }
            }
        };

        // Kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "event-consumer-group-0");
        DataStream<ObjectNode> source = env.addSource(
                new FlinkKafkaConsumer010<>("hes-sae-group-0", new JSONKeyValueDeserializationSchema(false), properties)
                        .assignTimestampsAndWatermarks(watermarkAssigner)
                        .setStartFromEarliest()
        )
                .name("Kafka Source")
                .uid("52a502a20ebadfdebdda5efe2032ac8f");

        source.keyBy(MainJob::getEventName)
                .process(new KeyedProcessFunction<String, ObjectNode, Object>() {
                    private MapState<String, AtomicLong> count;
                    private ValueState<Long> lastOutputTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        count = getRuntimeContext().getMapState(new MapStateDescriptor<>("eventCount", String.class, AtomicLong.class));
                        lastOutputTs = getRuntimeContext().getState(new ValueStateDescriptor<>("lastOutput", Long.class));
                    }

                    @Override
                    public void processElement(ObjectNode value, Context ctx, Collector<Object> out) throws Exception {
                        String eventName = value.findValue("event_name").asText("None");
                        if (!count.contains(eventName)) {
                            count.put(eventName, new AtomicLong(0L));
                        }
                        count.get(eventName).getAndIncrement();
                        Long lastTs = lastOutputTs.value() == null ? 0L : lastOutputTs.value();
                        if (ctx.timestamp() > lastTs + Time.seconds(5).toMilliseconds()) {
                            lastOutputTs.update(ctx.timestamp());
                            JSONObject output = new JSONObject();
                            count.entries().forEach(entry -> output.put(entry.getKey(), entry.getValue().get()));
                            out.collect(JSONObject.toJSONString(output, true));
                        }
                    }
                }).print();
        //source.transform("WatermarkObserver", TypeInformation.of(ObjectNode.class), new WatermarkObserver());

        env.execute("All in One");
    }

    public static String getEventName(ObjectNode data) {
        if (data == null) {
            return "None";
        }
        JsonNode value = data.findValue("event_name");
        if (value == null) {
            return "None";
        } else {
            return value.asText("None");
        }
    }

    public static class WatermarkObserver
            extends AbstractStreamOperator<ObjectNode>
            implements OneInputStreamOperator<ObjectNode, ObjectNode> {
        @Override
        public void processElement(StreamRecord<ObjectNode> element) throws Exception {
            System.out.println("GOT ELEMENT: " + element);
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            super.processWatermark(mark);
            System.out.println("GOT WATERMARK: " + mark);
        }
    }
}