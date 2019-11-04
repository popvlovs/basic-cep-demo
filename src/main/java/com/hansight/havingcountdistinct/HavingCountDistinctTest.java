package com.hansight.havingcountdistinct;

import com.hansight.util.ExpressionUtil;
import com.hansight.followedby.FollowedByKafkaMultiTopicTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class HavingCountDistinctTest {

    public static void main(String[] args) throws Exception {

        // 内部事件-主机特定端口被大量扫描
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ObjectNode> stream = env.addSource(new SourceFunction<ObjectNode>() {
            private volatile AtomicBoolean isRunning = new AtomicBoolean(true);

            @Override
            public void run(SourceContext<ObjectNode> out) throws Exception {
                int count = 0;
                while (isRunning.get()) {
                    while (count++ < 5) {
                        out.collect(getObjectNodeFromJson("{\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":" + (System.currentTimeMillis() + Time.seconds(count).toMilliseconds()) + ",\"dst_address\":\"8.8.4.4\",\"threat_feature\":\".P...........smarttender.biz.....\",\"src_port\":51944,\"event_name\":\"网络连接\",\"sensor_id\":\"16279cde-adb9-4e97-9efc-cb4cb756074a\",\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_alert\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"original_type\":\"YWxlcnQgZG5zICRIT01FX05FVCBhbnkgLT4gYW55IGFueSAobXNnOiJJTkZPIE9ic2VydmVkIEROUyBRdWVyeSB0byAuYml6IFRMRCI7IGRuc19xdWVyeTsgY29udGVudDoiLmJpeiI7IG5vY2FzZTsgaXNkYXRhYXQ6ITEscmVsYXRpdmU7IG1ldGFkYXRhOiBmb3JtZXJfY2F0ZWdvcnkgSU5GTzsgcmVmZXJlbmNlOnVybCx3d3cuc3BhbWhhdXMub3JnL3N0YXRpc3RpY3MvdGxkcy87IGNsYXNzdHlwZTpiYWQtdW5rbm93bjsgc2lkOjIwMjc4NjM7IHJldjoyOyBtZXRhZGF0YTphZmZlY3RlZF9wcm9kdWN0IEFueSwgYXR0YWNrX3RhcmdldCBDbGllbnRfRW5kcG9pbnQsIGRlcGxveW1lbnQgUGVyaW1ldGVyLCBzaWduYXR1cmVfc2V2ZXJpdHkgTWFqb3IsIGNyZWF0ZWRfYXQgMjAxOV8wOF8xMywgdXBkYXRlZF9hdCAyMDE5XzA5XzI4OykN\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"original_info\":\"vlABAAABAAAAAAAAC3NtYXJ0dGVuZGVyA2JpegAAAQAB\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}"));
                    }
                    Thread.sleep(10000);
                    out.collect(getObjectNodeFromJson("{\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":" + (System.currentTimeMillis() + Time.seconds(count).toMilliseconds()) + ",\"dst_address\":\"8.8.4.4\",\"threat_feature\":\".P...........smarttender.biz.....\",\"src_port\":51944,\"event_name\":\"网络连接\",\"sensor_id\":\"16279cde-adb9-4e97-9efc-cb4cb756074a\",\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_alert\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"original_type\":\"YWxlcnQgZG5zICRIT01FX05FVCBhbnkgLT4gYW55IGFueSAobXNnOiJJTkZPIE9ic2VydmVkIEROUyBRdWVyeSB0byAuYml6IFRMRCI7IGRuc19xdWVyeTsgY29udGVudDoiLmJpeiI7IG5vY2FzZTsgaXNkYXRhYXQ6ITEscmVsYXRpdmU7IG1ldGFkYXRhOiBmb3JtZXJfY2F0ZWdvcnkgSU5GTzsgcmVmZXJlbmNlOnVybCx3d3cuc3BhbWhhdXMub3JnL3N0YXRpc3RpY3MvdGxkcy87IGNsYXNzdHlwZTpiYWQtdW5rbm93bjsgc2lkOjIwMjc4NjM7IHJldjoyOyBtZXRhZGF0YTphZmZlY3RlZF9wcm9kdWN0IEFueSwgYXR0YWNrX3RhcmdldCBDbGllbnRfRW5kcG9pbnQsIGRlcGxveW1lbnQgUGVyaW1ldGVyLCBzaWduYXR1cmVfc2V2ZXJpdHkgTWFqb3IsIGNyZWF0ZWRfYXQgMjAxOV8wOF8xMywgdXBkYXRlZF9hdCAyMDE5XzA5XzI4OykN\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"original_info\":\"vlABAAABAAAAAAAAC3NtYXJ0dGVuZGVyA2JpegAAAQAB\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}"));
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

        stream.transform("WatermarkObserver", TypeInformation.of(ObjectNode.class), new FollowedByKafkaMultiTopicTest.WatermarkObserver());

        stream
                .filter(HavingCountDistinctTest::filterByCondition)
                .keyBy(new Tuple2KeySelector("dst_address", "dst_port"))
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .aggregate(new HavingCountDistinctTest.HavingCountDistinctAggregate(), new HavingCountDistinctTest.HavingCountProcessWindowFunction())
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

    private static class Tuple2KeySelector implements KeySelector<ObjectNode, Tuple2<String, String>> {
        private String[] fields;

        public Tuple2KeySelector(String... fields) {
            this.fields = fields;
        }

        @Override
        public Tuple2<String, String> getKey(ObjectNode value) throws Exception {
            return (Tuple2<String, String>) getFieldsAsText(value, fields);
        }
    }

    private static class HavingCountDistinctAggregate implements AggregateFunction<ObjectNode, Set<String>, Long> {
        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> add(ObjectNode value, Set<String> accumulator) {
            accumulator.add(getFieldAsText(value, "src_address"));
            return accumulator;
        }

        @Override
        public Long getResult(Set<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            a.addAll(b);
            return a;
        }
    }

    private static class HavingCountProcessWindowFunction extends ProcessWindowFunction<Long, Tuple4<String, Long, Long, Long>, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<Long> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
            Long count = elements.iterator().next();
            out.collect(new Tuple4<>(getTupleKeyAsString(key), context.window().getStart(), context.window().getEnd(), count));
        }
    }

    private static Tuple getFieldsAsText(ObjectNode val, String... fields) {
        int fieldNum = fields.length;
        Tuple tuple = Tuple.newInstance(fieldNum);
        for (int i = 0; i < fields.length; i++) {
            tuple.setField(getFieldAsText(val, fields[i]), i);
        }
        return tuple;
    }

    private static String getFieldAsText(ObjectNode val, String field) {
        JsonNode nodeVal = val.findValue(field);
        if (nodeVal == null) {
            return "None";
        }
        return nodeVal.asText("None");
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

    private static ObjectNode getObjectNodeFromJson(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.set("key", mapper.readValue(Long.toString(System.currentTimeMillis()).getBytes(), JsonNode.class));
        node.set("value", mapper.readValue(json.getBytes(), JsonNode.class));
        return node;
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
