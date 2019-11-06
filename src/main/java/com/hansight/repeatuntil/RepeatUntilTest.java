package com.hansight.repeatuntil;

import com.hansight.util.ExpressionUtil;
import com.hansight.util.MiscUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class RepeatUntilTest {

    public static void main(String[] args) throws Exception {

        // 内网主机遭受远程漏洞攻击后创建操作系统账号
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ObjectNode> stream = env.addSource(new SourceFunction<ObjectNode>() {
            private volatile AtomicBoolean isRunning = new AtomicBoolean(true);

            @Override
            public void run(SourceContext<ObjectNode> out) throws Exception {
                // src_address = 172.16.104.58
                out.collect(MiscUtil.getObjectNodeFromJson("{\"event_name\":\"网络连接\",\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":\" + System.currentTimeMillis() + \",\"dst_address\":\"8.8.4.4\",\"src_port\":51944,\"send_byte\": 1000001,\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_flow\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}", "occur_time"));
                Thread.sleep(2000);
                out.collect(MiscUtil.getObjectNodeFromJson("{\"event_name\":\"网络连接\",\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":\" + System.currentTimeMillis() + \",\"dst_address\":\"8.8.4.4\",\"src_port\":51944,\"send_byte\": 1000001,\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_flow\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}", "occur_time"));
                Thread.sleep(2000);
                // src_address = 172.16.104.85
                out.collect(MiscUtil.getObjectNodeFromJson("{\"event_name\":\"网络连接\",\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":\" + System.currentTimeMillis() + \",\"dst_address\":\"8.8.4.4\",\"src_port\":51944,\"send_byte\": 1000001,\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_flow\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.85\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}", "occur_time"));
                Thread.sleep(2000);
                out.collect(MiscUtil.getObjectNodeFromJson("{\"event_name\":\"网络连接\",\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":\" + System.currentTimeMillis() + \",\"dst_address\":\"8.8.4.4\",\"src_port\":51944,\"send_byte\": 1000001,\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_flow\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.85\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}", "occur_time"));
                Thread.sleep(2000);
                // dst_address = 172.16.104.58
                out.collect(MiscUtil.getObjectNodeFromJson("{\"event_name\":\"DNS查询\",\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":\" + System.currentTimeMillis() + \",\"dst_address\":\"172.16.104.58\",\"src_port\":51944,\"send_byte\": 1000001,\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_flow\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}", "occur_time"));
                Thread.sleep(5000);
                while (isRunning.get()){
                    out.collect(MiscUtil.getObjectNodeFromJson("{\"event_name\":\"DNS查询\",\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":\" + System.currentTimeMillis() + \",\"dst_address\":\"8.8.4.4\",\"src_port\":51944,\"send_byte\": 1000001,\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_flow\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}", "occur_time"));
                    Thread.sleep(2000);
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
                        Iterable<ObjectNode> prevEvents = context.getEventsForPattern("sub-pattern-A");
                        if (prevEvents.iterator().hasNext()) {
                            ObjectNode nodeA = prevEvents.iterator().next();
                            return ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && ExpressionUtil.equal(node, "event_digest", "nta_flow")
                                    && ExpressionUtil.equal(nodeA, "src_address", node, "dst_address");
                        }
                        return ExpressionUtil.equal(node, "event_name", "网络连接") && ExpressionUtil.belong(node, "src_address", "CSWLHT4101a6") && ExpressionUtil.equal(node, "dst_port", "53") && !ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && ExpressionUtil.equal(node, "event_digest", "nta_flow");
                    }
                })
                .timesOrMore(3)
                .followedBy("sub-pattern-B")
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode node, Context<ObjectNode> ctx) throws Exception {
                        // B, DNS查询, 源地址 belong 内网IP and not 目的地址 belong 内网IP
                        boolean expr = ExpressionUtil.equal(node, "event_name", "DNS查询") && ExpressionUtil.equal(node, "dst_port", "53") && ExpressionUtil.belong(node, "dst_address", "CSWLHT4101a6") && ExpressionUtil.gt(node, "send_byte", 1000000) && !ExpressionUtil.belong(node, "dst_address", "V3SD2MBU5b01") && ExpressionUtil.equal(node, "event_digest", "nta_flow");

                        // A.源地址 = B.源地址 and A.目的地址 = B.目的地址 and A.源端口 = B.源端口 and A.目的端口 = B.目的端口
                        Iterable<ObjectNode> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                        ObjectNode nodeA = prevEvents.iterator().next();
                        return expr && ExpressionUtil.equal(nodeA, "src_address", node, "dst_address");
                    }
                })
                .within(Time.seconds(10));

        SingleOutputStreamOperator output = CEP.pattern(stream, pattern)
                .select(matchedEvents -> {
                    StringBuilder sb = new StringBuilder();
                    for (String patternName : matchedEvents.keySet()) {
                        sb.append(patternName + ": ");
                        List<ObjectNode> events = matchedEvents.get(patternName);
                        String vals = events.stream().map(ObjectNode::toString).reduce((a, b) -> a + ',' + b).orElse("");
                        sb.append("[" + vals + "] ");
                    }
                    return sb.toString();
                })
                .name("Pattern-RepeatUntil")
                .uid("Pattern-RepeatUntil");

        output.print();

        MiscUtil.printRecordAndWatermark(stream);

        env.execute("Repeat Until Streaming Job");
    }
}
