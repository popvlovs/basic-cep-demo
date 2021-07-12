package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonSource;
import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class MultipleRules {
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

        for (int i = 0; i < 1; ++i) {
            addHavingCountTask(stream);
        }
        env.execute("Benchmark");
    }

    private static void addHavingCountTask(DataStream<JSONObject> stream) {
        stream
                .filter(MultipleRules::filterByCondition)
                .keyBy(node -> ExpressionUtil.getFieldAsText(node, "src_address", "None"))
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new MultipleRules.HavingCountAggregate(), new MultipleRules.HavingCountProcessWindowFunction())
                .addSink(new DiscardingSink<>());
    }

    private static boolean filterByCondition(JSONObject node) {
        return ExpressionUtil.getFieldAsText(node, "event_name", "None").equals("邮件发送");
    }

    private static class HavingCountAggregate implements AggregateFunction<JSONObject, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(JSONObject value, Long accumulator) {
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
        public void process(String key, Context context, java.lang.Iterable<Long> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
            Long count = elements.iterator().next();
            out.collect(new Tuple4<>(key, context.window().getStart(), context.window().getEnd(), count));
        }
    }
}
