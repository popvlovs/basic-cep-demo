package com.hansight.inmemorysource.source;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonStringSource;
import com.hansight.util.ExpressionUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class KeyedMapWithJSONDeserialization {
    public static void main(String[] args) throws Exception {
        /**
         * Case 1
         *  parallelism: 5
         *  total records: 75000000
         *  time to finish: 173 sec
         *  records out per second: 433526
         *
         * Case 2
         *  parallelism: 1
         *  total records: 15000000
         *  time to finish: 151 sec
         *  records out per second: 99337
         *
         * Conclusion:
         *  Performance reduced 50%
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonStringSource(15_000_000, 10));
        stream.map(element -> element.fluentRemove("payload"))
                .keyBy(element -> ExpressionUtil.getFieldAsText(element, "key", "None"))
                .addSink(new DiscardingSink<>());

        env.execute("Benchmark");
    }
}
