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
public class CompositeFollowedBy {

    /**
     * There is something wrong with this job
     * 观测到的现象：
     *      首先Source端backpressure=1
     *      一段时间后TaskManager无响应
     *      观察对应JVM进程，发现内存溢出，OldGeneration 99.9%，程序频繁Full GC，判断是内存溢出（JVM有16G内存）
     * 未证实猜想：
     *      模拟数据源产生的数据时间戳特别集中，假设eps有400k/s
     *      此时CEP Pattern的两个终止条件【meet event B】和【event time out of within】都无法成立
     *      因此state中累积了大量的事件，默认情况下state是in-heap的，这会导致JVM内存不足，并频繁的full-gc（当然也释放不了）
     *      但这个数据量的事件并不会很大（从界面上看大概60w+数据被transfer到CepOperator），应该小于100MB，理论上不会导致内存不足才对
     * 尝试：
     *      用SQL来等价实现，确认是否有同样的性能问题，具体实现见SQLFollowedBy
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // For debug
        env.setParallelism(1);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(15_000_000, 10))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(JSONObject element) {
                        return element.getLongValue("occur_time");
                    }
                });

        Condition conditionA = ConditionUtil.fromFilter("event_name = 邮件登陆");
        Condition conditionB = ConditionUtil.fromFilter("event_name = 邮件发送");

        stream = stream.map(element -> element.fluentRemove("payload"));

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>
                begin("A", skipStrategy).where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject val, Context<JSONObject> context) throws Exception {
                        return conditionA.eval(val);
                    }
                })
                .followedBy("B").where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject val, Context<JSONObject> ctx) throws Exception {
                        return conditionB.eval(val);
                    }
                })
                .within(Time.seconds(10));

        CEP.pattern(stream, pattern)
                .select(CompositeFollowedBy::select)
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
