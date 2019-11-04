package com.hansight.notfollowedby;

import com.hansight.util.ExpressionUtil;
import com.hansight.util.MiscUtil;
import com.hansight.util.OutputUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class NotFollowedByTest {

    public static void main(String[] args) throws Exception {

        // 内网主机发起异常DNS通信-异常DNS协议报文
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
                    Thread.sleep(11000);
                    out.collect(getObjectNodeFromJson("{\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":" + System.currentTimeMillis() + ",\"dst_address\":\"8.8.4.4\",\"threat_feature\":\".P...........smarttender.biz.....\",\"src_port\":51944,\"event_name\":\"网络连接\",\"sensor_id\":\"16279cde-adb9-4e97-9efc-cb4cb756074a\",\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_alert\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"original_type\":\"YWxlcnQgZG5zICRIT01FX05FVCBhbnkgLT4gYW55IGFueSAobXNnOiJJTkZPIE9ic2VydmVkIEROUyBRdWVyeSB0byAuYml6IFRMRCI7IGRuc19xdWVyeTsgY29udGVudDoiLmJpeiI7IG5vY2FzZTsgaXNkYXRhYXQ6ITEscmVsYXRpdmU7IG1ldGFkYXRhOiBmb3JtZXJfY2F0ZWdvcnkgSU5GTzsgcmVmZXJlbmNlOnVybCx3d3cuc3BhbWhhdXMub3JnL3N0YXRpc3RpY3MvdGxkcy87IGNsYXNzdHlwZTpiYWQtdW5rbm93bjsgc2lkOjIwMjc4NjM7IHJldjoyOyBtZXRhZGF0YTphZmZlY3RlZF9wcm9kdWN0IEFueSwgYXR0YWNrX3RhcmdldCBDbGllbnRfRW5kcG9pbnQsIGRlcGxveW1lbnQgUGVyaW1ldGVyLCBzaWduYXR1cmVfc2V2ZXJpdHkgTWFqb3IsIGNyZWF0ZWRfYXQgMjAxOV8wOF8xMywgdXBkYXRlZF9hdCAyMDE5XzA5XzI4OykN\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"original_info\":\"vlABAAABAAAAAAAAC3NtYXJ0dGVuZGVyA2JpegAAAQAB\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}"));
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

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<ObjectNode, ObjectNode> pattern = Pattern.<ObjectNode>
                begin("sub-pattern-A", skipStrategy)
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode node, Context<ObjectNode> context) throws Exception {
                        // A, 网络连接, 源地址 belong 内网IP and 目的端口 = 53 and not 目的地址 belong 内网IP and 发送流量 > 1000000 and not 目的地址 belong 保留IP地址列表 and 事件摘要 = "nta_flow"
                        return ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && !ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && !ExpressionUtil.equal(node, "event_digest", "nta_flow");
                    }
                })
                .followedBy("sub-pattern-B")
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode node, Context<ObjectNode> ctx) throws Exception {
                        // B, DNS查询, 源地址 belong 内网IP and not 目的地址 belong 内网IP
                        boolean expr = ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6");

                        // A.源地址 = B.源地址 and A.目的地址 = B.目的地址 and A.源端口 = B.源端口 and A.目的端口 = B.目的端口
                        Iterable<ObjectNode> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                        ObjectNode nodeA = prevEvents.iterator().next();
                        boolean relationExpr = ExpressionUtil.equal(nodeA, "src_address", node, "src_address") && ExpressionUtil.equal(nodeA, "dst_address", node, "dst_address") && ExpressionUtil.equal(nodeA, "src_port", node, "src_port") && ExpressionUtil.equal(nodeA, "dst_port", node, "dst_port");

                        return expr && relationExpr;
                    }
                })
                .within(Time.seconds(10));

        final OutputTag<String> outputTag = new OutputTag<String>("not-followed-by-side-output") {
        };
        SingleOutputStreamOperator<String> output = CEP.pattern(stream, pattern)
                .flatSelect(
                        outputTag,
                        new PatternFlatTimeoutFunction<ObjectNode, String>() {
                            @Override
                            public void timeout(Map<String, List<ObjectNode>> patterns,
                                                long timeoutTimestamp,
                                                Collector<String> collector) throws Exception {
                                collector.collect(OutputUtil.asText(patterns));
                            }
                        },
                        new PatternFlatSelectFunction<ObjectNode, String>() {
                            @Override
                            public void flatSelect(Map<String, List<ObjectNode>> patterns, Collector<String> collector) throws Exception {
                                // do nothing...
                            }
                        })
                .name("Pattern-NotFollowedBy")
                .uid("Pattern-NotFollowedBy");

        output.getSideOutput(outputTag).print();
        MiscUtil.printRecordAndWatermark(stream);

        env.execute("Not Followed By Streaming Job");
    }

    private static ObjectNode getObjectNodeFromJson(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.set("key", mapper.readValue(Long.toString(System.currentTimeMillis()).getBytes(), JsonNode.class));
        node.set("value", mapper.readValue(json.getBytes(), JsonNode.class));
        return node;
    }
}
