package com.hansight.util;

import com.alibaba.fastjson.JSONObject;
import com.hansight.followedby.FollowedByKafkaMultiTopicTest;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/4
 */
public class MiscUtil {

    public static DataStream<ObjectNode> mockPeriodicEventSource(StreamExecutionEnvironment env, String event, String timeField) {
        return env.addSource(new SourceFunction<ObjectNode>() {
            private volatile AtomicBoolean isRunning = new AtomicBoolean(true);

            @Override
            public void run(SourceContext<ObjectNode> out) throws Exception {
                int count = 0;
                while (isRunning.get()) {
                    if (count++ < 5) {
                        out.collect(getObjectNodeFromJson(event, timeField));
                        Thread.sleep(1000);
                    } else {
                        out.collect(getObjectNodeFromJson(event, timeField));
                        Thread.sleep(10000);
                    }
                }
            }

            @Override
            public void cancel() {
                isRunning.set(false);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(3000)) {
            @Override
            public long extractTimestamp(ObjectNode element) {
                JsonNode timestamp = element.findValue(timeField);
                if (timestamp == null) {
                    return System.currentTimeMillis();
                } else {
                    return timestamp.asLong(0);
                }
            }
        });
    }

    public static void printRecordAndWatermark(DataStream stream) {
        stream.transform("WatermarkObserver", TypeInformation.of(ObjectNode.class), new FollowedByKafkaMultiTopicTest.WatermarkObserver());
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

    private static ObjectNode getObjectNodeFromJson(String json, String timeField) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        JSONObject jsonObj = JSONObject.parseObject(json);
        jsonObj.put(timeField, System.currentTimeMillis());
        node.set("key", mapper.readValue(Long.toString(System.currentTimeMillis()).getBytes(), JsonNode.class));
        node.set("value", mapper.readValue(jsonObj.toJSONString().getBytes(), JsonNode.class));
        return node;
    }
}
