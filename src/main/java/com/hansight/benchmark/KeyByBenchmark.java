package com.hansight.benchmark;

import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingTupleSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByBenchmark {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new IncreasingTupleSource(15_000_000, 10));

        stream
                .keyBy(0)
                .addSink(new DiscardingSink<>());

        env.execute("Key by Benchmark");
    }
}
