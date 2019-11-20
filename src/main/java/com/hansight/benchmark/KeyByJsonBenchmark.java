package com.hansight.benchmark;

import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingJsonSource;
import com.hansight.source.IncreasingTupleSource;
import com.hansight.util.ExpressionUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByJsonBenchmark {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ObjectNode> stream = env.addSource(new IncreasingJsonSource(15_000_000, 10));

        stream
                .keyBy(val -> ExpressionUtil.getFieldAsText(val, "key", "None"))
                .addSink(new DiscardingSink<>());

        env.execute("Key by Benchmark");
    }
}
