package com.hansight.havingcount;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicBoolean;

public class HavingCountTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ObjectNode> stream = env.addSource(new SourceFunction<ObjectNode>() {
            private volatile AtomicBoolean isRunning = new AtomicBoolean(true);

            @Override
            public void run(SourceContext<ObjectNode> out) throws Exception {
                int count = 0;
                while (isRunning.get()) {
                    if (count++ < 5) {
                        out.collect(getObjectNodeFromJson("{\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":" + System.currentTimeMillis() + ",\"dst_address\":\"8.8.4.4\",\"threat_feature\":\".P...........smarttender.biz.....\",\"src_port\":51944,\"event_name\":\"网络连接\",\"sensor_id\":\"16279cde-adb9-4e97-9efc-cb4cb756074a\",\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_alert\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"original_type\":\"YWxlcnQgZG5zICRIT01FX05FVCBhbnkgLT4gYW55IGFueSAobXNnOiJJTkZPIE9ic2VydmVkIEROUyBRdWVyeSB0byAuYml6IFRMRCI7IGRuc19xdWVyeTsgY29udGVudDoiLmJpeiI7IG5vY2FzZTsgaXNkYXRhYXQ6ITEscmVsYXRpdmU7IG1ldGFkYXRhOiBmb3JtZXJfY2F0ZWdvcnkgSU5GTzsgcmVmZXJlbmNlOnVybCx3d3cuc3BhbWhhdXMub3JnL3N0YXRpc3RpY3MvdGxkcy87IGNsYXNzdHlwZTpiYWQtdW5rbm93bjsgc2lkOjIwMjc4NjM7IHJldjoyOyBtZXRhZGF0YTphZmZlY3RlZF9wcm9kdWN0IEFueSwgYXR0YWNrX3RhcmdldCBDbGllbnRfRW5kcG9pbnQsIGRlcGxveW1lbnQgUGVyaW1ldGVyLCBzaWduYXR1cmVfc2V2ZXJpdHkgTWFqb3IsIGNyZWF0ZWRfYXQgMjAxOV8wOF8xMywgdXBkYXRlZF9hdCAyMDE5XzA5XzI4OykN\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"original_info\":\"vlABAAABAAAAAAAAC3NtYXJ0dGVuZGVyA2JpegAAAQAB\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}"));
                    }
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning.set(false);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(3000)) {
            @Override
            public long extractTimestamp(ObjectNode element) {
                JsonNode timestamp = element.findValue("occur_time");
                if (timestamp == null) {
                    return System.currentTimeMillis();
                } else {
                    return timestamp.asLong(0);
                }
            }
        });

        stream
                .filter(HavingCountTest::filterByCondition)
                .keyBy(node -> getFieldAsText(node, "src_address"))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .aggregate(new HavingCountAggregate(), new HavingCountProcessWindowFunction())
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

    private static class HavingCountAggregate implements AggregateFunction<ObjectNode, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ObjectNode value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class HavingCountProcessWindowFunction extends ProcessWindowFunction<Long, Tuple4<String, Long, Long, Long>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
            Long count = elements.iterator().next();
            if (count >= 3) {
                out.collect(new Tuple4<>(key, context.window().getStart(), context.window().getEnd(), count));
            }
        }
    }

    private static String getFieldAsText(ObjectNode val, String field) {
        JsonNode nodeVal = val.findValue(field);
        if (nodeVal == null) {
            return "None";
        }
        return nodeVal.asText("None");
    }

    private static ObjectNode getObjectNodeFromJson(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.set("key", mapper.readValue(Long.toString(System.currentTimeMillis()).getBytes(), JsonNode.class));
        node.set("value", mapper.readValue(json.getBytes(), JsonNode.class));
        return node;
    }
}
