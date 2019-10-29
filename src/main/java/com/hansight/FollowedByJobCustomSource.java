package com.hansight;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FollowedByJobCustomSource {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> allInOne = env.fromElements("A:1", "A:2", "A:3", "B:1", "B:2", "B:3");

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToNext();
        Pattern<String, String> pattern = Pattern.<String>
                begin("sub-pattern-A", skipStrategy)
                .where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String val, Context<String> context) throws Exception {
                        return val.startsWith("A");
                    }
                })
                .followedBy("sub-pattern-B")
                .where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String val, Context<String> ctx) throws Exception {
                        Iterable<String> prevEvents = ctx.getEventsForPattern("sub-pattern-A");
                        String prevA = prevEvents.iterator().next();
                        return val.startsWith("B") && val.endsWith(prevA.split(":")[1]);
                    }
                });

        PatternStream<String> patternStream = CEP.pattern(allInOne, pattern);
        patternStream.select(JSONObject::toJSONString).print();

        env.execute("Followed By Streaming Job");
    }
}
