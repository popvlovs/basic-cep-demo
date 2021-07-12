package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonSource;
import com.hansight.util.ExpressionUtil;
import com.hansight.util.condition.Condition;
import com.hansight.util.condition.ConditionUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class SimpleFollowedBy {

    /**
     * There is something wrong with this job
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(15_000_000, 10))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(JSONObject element) {
                        return element.getLongValue("occur_time");
                    }
                });

        stream = stream.map(element -> element.fluentRemove("payload"))
                .keyBy(element -> element.getIntValue("key"));

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>
                begin("A", skipStrategy)
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject val, Context<JSONObject> context) throws Exception {
                        return val.getString("event_name").equals("邮件登陆");
                    }
                }).oneOrMore().greedy()
                .followedBy("B")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject val, Context<JSONObject> ctx) throws Exception {
                        return val.getString("event_name").equals("邮件发送");
                    }
                })
                .within(Time.seconds(10));

        CEP.pattern(stream, pattern)
                .select(SimpleFollowedBy::select)
                .addSink(new DiscardingSink<>());

        env.execute("Benchmark");
    }

    private static String select(Map<String, List<JSONObject>> matchedEvents) {
        StringBuilder sb = new StringBuilder();
        for (String patternName : matchedEvents.keySet()) {
            sb.append(patternName).append(": ");
            List<JSONObject> events = matchedEvents.get(patternName);
            sb.append("[").append(events.stream().map(event -> event.toJSONString()).reduce((a, b) -> a + ',' + b).orElse("")).append("]");
        }
        return sb.toString();
    }
}
