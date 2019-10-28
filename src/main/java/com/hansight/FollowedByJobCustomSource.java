package com.hansight;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class FollowedByJobCustomSource {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> allInOne = env.fromElements("A:1", "A:2", "A:3", "B:1", "B:2", "B:3");
        final OutputTag<String> outputA = new OutputTag<String>("A-output") {
        };
        final OutputTag<String> outputB = new OutputTag<String>("B-output") {
        };
        SingleOutputStreamOperator<String> sourceToSplit = allInOne.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
                if (value.startsWith("A")) {
                    ctx.output(outputA, value);
                }
                if (value.startsWith("B")) {
                    ctx.output(outputB, value);
                }
            }
        });

        DataStream<String> streamA = sourceToSplit.getSideOutput(outputA);
        DataStream<String> streamB = sourceToSplit.getSideOutput(outputB);

        DataStream<String> streamA2 = env.fromElements("A:1", "A:2", "A:3");
        DataStream<String> streamB2 = env.fromElements("B:1", "B:2", "B:3");
        //streamA.print();
        //streamB.print();
        //streamA.union(streamB).print();
        //streamA.union(streamB).print();

        SplitStream<String> split = allInOne.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                List<String> output = new ArrayList<>();
                if (value.startsWith("A")) {
                    output.add("output-A");
                }
                if (value.startsWith("B")) {
                    output.add("output-B");
                }
                return output;
            }
        });
        DataStream<String> splitA = split.select("output-A");
        DataStream<String> splitB = split.select("output-B");
        splitA.print();
        splitB.print();

        env.execute("Followed By Streaming Job");
    }
}
