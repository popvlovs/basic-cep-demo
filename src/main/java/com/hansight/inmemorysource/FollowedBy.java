package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class FollowedBy {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * {"event_name": "邮件登陆", "src_address": "1.1.1.1", "occur_time": 1574934481585}
         * {"event_name": "邮件发送", "src_address": "2.2.2.2", "occur_time": 1574934481586}
         */
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
                begin("A", skipStrategy).where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return StringUtils.equals(value.getString("event_name"), "邮件登陆");
                    }
                })
                .followedBy("B").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return StringUtils.equals(value.getString("event_name"), "邮件发送");
                    }
                })
                .within(Time.seconds(10));

        CEP.pattern(stream, pattern)
                .select(FollowedBy::select)
                .print();

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
