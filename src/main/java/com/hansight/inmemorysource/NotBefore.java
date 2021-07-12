package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.notbefore.function.KeyedNotBeforeFunctionBuilder;
import com.hansight.notbefore.function.NotBeforeFunctionBuilder;
import com.hansight.notbefore.function.NotBeforeJsonFunction;
import com.hansight.source.IncreasingFastJsonSource;
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
public class NotBefore {
    public static void main(String[] args) throws Exception {
        /**
         * Case 1
         *  parallelism: 5
         *  total records: 75000000
         *  time to finish: 53 sec
         *  records out per second: 1415094
         *
         * Case 2
         *  parallelism: 1
         *  total records: 15000000
         *  time to finish: 46 sec
         *  records out per second: 326086
         *
         * Conclusion
         *  类似于KeyedNotBefore，NotBefore的性能也比KeyedMap好上许多，应该也是性能波动的原因
         *  复测一下NotKeyedMap的性能，在p=5，p=1两种情况下，eps分别为1562500和333333
         *  从结论来看，非Keyed的NotBeforeFunction对性能影响也不大（A很多而B很少的场景），p=1,p=5的性能下降了分别在9.5%和2%（考虑到性能波动，可以认为大部分情况下小于 5%）
         *
         * Case 3
         *  之前的测试未将process并行度设为1，调整后补充测试结果如下
         *  parallelism: 5
         *  total records: 75000000
         *  time to finish: 236 sec
         *  records out per second: 317796
         *
         *  parallelism: 1
         *  total records: 15000000
         *  time to finish: 74 sec
         *  records out per second: 202702
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
        stream.map(element -> element.fluentRemove("payload"))
                .process(NotBeforeFunctionBuilder.builder()
                        .filterByA("event_name = 邮件登陆 or src_address belong 内网IP")
                        .filterByB("event_name = 邮件发送")
                        .groupByA("src_address")
                        .groupByB("dst_address")
                        .build())
                .setParallelism(1)
                .addSink(new DiscardingSink<>());

        env.execute("Benchmark");
    }
}
