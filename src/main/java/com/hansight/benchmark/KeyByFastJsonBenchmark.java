package com.hansight.benchmark;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByFastJsonBenchmark {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(15_000_000, 10));

        stream
                .map(val -> val.fluentRemove("payload"))
                .keyBy(val -> val.getString("key"))
                .addSink(new DiscardingSink<>());

        env.execute("Key by Benchmark");
    }
}
