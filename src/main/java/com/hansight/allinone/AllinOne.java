package com.hansight.allinone;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.List;
import java.util.Properties;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/6
 * @description .
 */
public class AllinOne {

    public static void main(String[] args) throws Exception {
        // 数据源
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Benchmark测试，parallelism = 3 eps ≈ 7w
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.193:9092");
        properties.setProperty("group.id", "flink-consumer-group-3");
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


        // Having-Any：内部邮件服务器向黑名单IP发送邮件
        // Benchmark测试，单Operator + Source，parallelism = 3，eps ≈ 6.5w
        stream
                .filter(node -> ExpressionUtil.equal(node, "event_name", "网络连接")
                        && ExpressionUtil.belong(node, "src_address", "邮件服务器地址")
                        && ExpressionUtil.belong(node, "dst_address", "黑名单IP")
                        && (ExpressionUtil.equal(node, "app_protocol", "smtp") || ExpressionUtil.equal(node, "dst_port", "25"))
                        && !ExpressionUtil.belong(node, "dst_address", "保留IP地址列表"))
                .name("Having Any #1")
                .print();

        // Followed-By：内网主机遭受远程漏洞攻击后创建操作系统账号
        // Benchmark测试，单Operator + Source，parallelism = 3，eps ≈ 1.5w (初期4.8k)，back-pressure ≈ 0.8
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToNext();
        Pattern<ObjectNode, ObjectNode> pattern = Pattern.<ObjectNode>
                begin("sub-pattern-A", skipStrategy)
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode val, Context<ObjectNode> context) throws Exception {
                        // 全局事件A：目的地址 belong 内网IP and (事件名称 = "远程漏洞攻击" or (事件摘要 = "nta_alert" and 事件名称 like "漏洞"))
                        boolean expr = ExpressionUtil.belong(val, "dst_address", "内网IP") && (ExpressionUtil.equal(val, "event_name", "远程漏洞攻击") || (ExpressionUtil.equal(val, "event_digest", "nta_alert") && ExpressionUtil.like(val, "event_name", "漏洞")));
                        return expr;
                    }
                })
                .followedBy("sub-pattern-B")
                .where(new IterativeCondition<ObjectNode>() {
                    @Override
                    public boolean filter(ObjectNode val, Context<ObjectNode> ctx) throws Exception {
                        // 全局事件B：(Windows事件ID = 4720 or (事件名称 = "账号创建" and 数据源 like "Linux")) and 目的地址 belong 内网IP
                        boolean expr = (ExpressionUtil.equal(val, "windows_event_id", 4720) || (ExpressionUtil.equal(val, "event_name", "账号创建") && ExpressionUtil.like(val, "data_source", "Linux"))) && ExpressionUtil.belong(val, "dst_address", "内网IP");
                        Iterable<ObjectNode> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                        if (prevEvents != null && prevEvents.iterator().hasNext()) {
                            ObjectNode eventA = prevEvents.iterator().next();
                            boolean relationExpr = ExpressionUtil.equal(val, "dst_address", eventA, "src_address");
                            return expr && relationExpr;
                        }
                        return false;
                    }
                })
                .within(Time.seconds(10));
        CEP.pattern(stream, pattern)
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
                .name("Followed-By #1")
                .print();

        env.execute("All in One Streaming Job");
    }
}
