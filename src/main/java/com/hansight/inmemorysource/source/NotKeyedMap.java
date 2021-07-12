package com.hansight.inmemorysource.source;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class NotKeyedMap {
    public static void main(String[] args) throws Exception {
        /**
         * Case 1
         *  parallelism: 5
         *  total records: 75000000
         *  time to finish: 94 sec
         *  records out per second: 797872
         *
         * Case 2
         *  parallelism: 1
         *  total records: 15000000
         *  time to finish: 70 sec
         *  records out per second: 214285
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(15_000_000, 10));
        stream.map(element -> element.fluentRemove("payload"))
                .addSink(new DiscardingSink<>());

        env.execute("Benchmark");
    }
}
