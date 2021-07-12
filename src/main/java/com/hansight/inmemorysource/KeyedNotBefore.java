package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.notbefore.function.KeyedNotBeforeFunctionBuilder;
import com.hansight.notbefore.function.KeyedNotBeforeJsonFunction;
import com.hansight.source.IncreasingFastJsonSource;
import com.hansight.util.ExpressionUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class KeyedNotBefore {
    public static void main(String[] args) throws Exception {
        /**
         * Case 1
         *  parallelism: 5
         *  total records: 75000000
         *  time to finish: 76 sec
         *  records out per second: 986842
         *
         * Case 2
         *  parallelism: 1
         *  total records: 15000000
         *  time to finish: 46 sec
         *  records out per second: 326086
         *
         * Conclusion
         *  比起什么都不处理的KeyedMap，接了一个process function的NotBefore居然还快了不少，只能解释为环境导致的性能波动
         *  因此又复测了一下KeyedMap的性能，在p=5，p=1两种情况下，eps分别为1013513和357142
         *  从结论来看，KeyedNotBeforeFunction对性能影响不大（A很多而B很少的场景），大概使性能下降了3%左右
         *  补充测试了一下assignTimestampsAndWatermarks对性能的影响——完全没有
         *
         *  更改了一下keyBy的处理逻辑，以期将所有的not before规则都统一到keyBy的场景下
         *  更改了process function的实现和匹配逻辑，支持从真实hql中生成process function
         *  测试了一下该情况下的性能，相比之前的结果，有较明显的下降，具体原因分析中
         *
         *    parallelism: 5
         *    total records: 75000000
         *    time to finish: 147 sec
         *    records out per second: 510204
         *
         *    parallelism: 1
         *    total records: 15000000
         *    time to finish: 116 sec
         *    records out per second: 129310
         *
         *  稍微分析了一下原因，大概还是和性能波动有关，实测下来增加 filter + keyBy + process对性能影响不大，方案可行
         *  至于性能波动的原因，猜测可能和虚拟机资源分配有关？——主要指CPU计算资源
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(15_000_000, 10))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(JSONObject element) {
                        return element.getLongValue("occur_time");
                    }
                });

        KeyedNotBeforeJsonFunction notBeforeFunction = KeyedNotBeforeFunctionBuilder.builder()
                .filterByA("event_name = 邮件登陆")
                .filterByB("event_name = 邮件发送")
                .groupByA("src_address")
                .groupByB("dst_address")
                .build();

        stream.map(element -> element.fluentRemove("payload"))
                .filter(notBeforeFunction::isInterestedIn)
                .keyBy(notBeforeFunction::extractGroupSignature)
                .process(notBeforeFunction)
                .addSink(new DiscardingSink<>());

        env.execute("Benchmark");
    }
}
