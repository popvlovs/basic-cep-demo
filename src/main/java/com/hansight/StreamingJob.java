/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hansight;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.*;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Samples:
            {"username": "yitian_song", "eventId": "9178711578", "region": "shanghai", "eventTime": 1568020959959, "action": "ChangePassword"}
            {"username": "xianzheng_guo", "eventId": "8749372921", "region": "chengdu", "eventTime": 1568020958157, "action": "Login"}
            {"username": "chunhua_wang", "eventId": "8199604628", "region": "chengdu", "eventTime": 1568020959727, "action": "ChangePassword"}
            {"username": "yitian_song", "eventId": "7369951365", "region": "chengdu", "eventTime": 1568020958276, "action": "Login"}
            {"username": "chunhua_wang", "eventId": "8831629920", "region": "nanjing", "eventTime": 1568020958151, "action": "Logout"}
            {"username": "yitian_song", "eventId": "9374697438", "region": "shanghai", "eventTime": 1568020959680, "action": "Logout"}
            {"username": "yitian_song", "eventId": "9881932069", "region": "shanghai", "eventTime": 1568020959906, "action": "Login"}
            {"username": "chunhua_wang", "eventId": "6114486188", "region": "beijing", "eventTime": 1568020958262, "action": "ChangePassword"}
            {"username": "chunhua_wang", "eventId": "1871315262", "region": "shanghai", "eventTime": 1568020958038, "action": "Login"}
            {"username": "yitian_song", "eventId": "7353976397", "region": "nanjing", "eventTime": 1568020959816, "action": "Logout"}
            {"username": "yitian_song", "eventId": "3394905645", "region": "nanjing", "eventTime": 1568020959228, "action": "ChangePassword"}
            {"username": "chunhua_wang", "eventId": "6524301532", "region": "chengdu", "eventTime": 1568020958707, "action": "ChangePassword"}
            {"username": "xianzheng_guo", "eventId": "2065104030", "region": "shanghai", "eventTime": 1568020958529, "action": "ChangePassword"}
            {"username": "xianzheng_guo", "eventId": "7595657870", "region": "shanghai", "eventTime": 1568020959396, "action": "Login"}
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.146:9092");
        properties.setProperty("group.id", "test-consumer-group");
        // 最大可接受的乱序时间范围
        long maxTolerableDelayMillis = 3000;
        DataStream<Map> stream = env.addSource(new FlinkKafkaConsumer010<>("userActionsV3", new DeserializationSchema<Map>() {
            @Override
            public Map deserialize(byte[] message) throws IOException {
                String data = new String(message);
                return JSONObject.parseObject(data);
            }

            @Override
            public boolean isEndOfStream(Map nextElement) {
                return false;
            }

            @Override
            public TypeInformation<Map> getProducedType() {
                return TypeExtractor.getForClass(Map.class);
            }
        }, properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Map>(Time.milliseconds(maxTolerableDelayMillis)) {
                    @Override
                    public long extractTimestamp(Map element) {
                        String timestamp = ((JSONObject) element).getString("eventTime");
                        return parseTime(timestamp);
                    }
                })
                .setStartFromEarliest());

        // 1. Not followed by
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<Map, Map> pattern = Pattern.<Map>
                begin("A", skipStrategy)
                .where(new IterativeCondition<Map>() {
                    @Override
                    public boolean filter(Map action, Context<Map> context) throws Exception {
                        return !StringUtils.equals(((JSONObject) action).getString("action"), "LoginXXX");
                    }
                })
                .followedBy("B")
                .where(new IterativeCondition<Map>() {
                    @Override
                    public boolean filter(Map action, Context<Map> context) throws Exception {
                        return StringUtils.equals(((JSONObject) action).getString("action"), "Login");
                    }
                })
                .within(Time.seconds(10));

        PatternStream<Map> patternStream = CEP.pattern(
                stream.keyBy(data -> ((JSONObject) data).getString("username")),
                pattern);

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        DataStream<String> dataStream = patternStream.flatSelect(
                outputTag,
                new PatternFlatTimeoutFunction<Map, String>() {
                    @Override
                    public void timeout(Map<String, List<Map>> patterns,
                                        long timeoutTimestamp,
                                        Collector<String> collector) throws Exception {
                        String output = JSONObject.toJSONString(patterns);
                        collector.collect(output);
                    }
                },
                new PatternFlatSelectFunction<Map, String>() {
                    @Override
                    public void flatSelect(Map<String, List<Map>> patterns, Collector<String> collector) throws Exception {
                        String output = JSONObject.toJSONString(patterns);
                        collector.collect(output);
                    }
                }

        ).uid("Pattern-NotFollowedBy");

        DataStream<String> notFollowedBy = ((SingleOutputStreamOperator<String>) dataStream).getSideOutput(outputTag);

        // 2. Repeat until
        Pattern<Map, Map> repeatUntilPattern = Pattern.<Map>
                begin("repeatUntil", skipStrategy)
                .where(new IterativeCondition<Map>() {
                    @Override
                    public boolean filter(Map action, Context<Map> context) throws Exception {
                        return !StringUtils.equals(((JSONObject) action).getString("action"), "Login");
                    }
                })
                .timesOrMore(3)
                .greedy()
                .until(new IterativeCondition<Map>() {
                    @Override
                    public boolean filter(Map action, Context<Map> context) throws Exception {
                        return StringUtils.equals(((JSONObject) action).getString("action"), "Logout");
                    }
                })
                .within(Time.seconds(10));

        PatternStream<Map> repeatUntilPatternStream = CEP.pattern(
                stream.keyBy(data -> ((JSONObject) data).getString("username")),
                repeatUntilPattern);

        DataStream<String> repeatUntil = repeatUntilPatternStream.select(new PatternSelectFunction<Map, String>() {
            @Override
            public String select(Map<String, List<Map>> map) throws Exception {
                return JSONObject.toJSONString(map);
            }
        }).uid("Pattern-RepeatUntil");


        // 3. Window count
        DataStream<String> windowCount = stream
                .keyBy(data -> ((JSONObject) data).getString("username"))
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<Map, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Map> elements, Collector<String> out) throws Exception {
                        long count = 0;
                        for (Map element : elements) {
                            count++;
                        }
                        out.collect("Window: " + context.window() + ", Count: " + count);
                    }
                })
                .uid("Pattern-WindowCount");

        // 4. Not before
        DataStream<String> notBefore = stream
                .keyBy(data -> ((JSONObject) data).getString("username"))
                .process(new KeyedProcessFunction<String, Map, String>() {
                    private ValueState<Map> lastA;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastA = getRuntimeContext().getState(new ValueStateDescriptor<>("lastA", Map.class));
                    }

                    @Override
                    public void processElement(Map value, Context ctx, Collector<String> out) throws Exception {
                        // On event A
                        if (value != null && StringUtils.equalsIgnoreCase(((JSONObject) value).getString("action"), "Login")) {
                            lastA.update(value);
                        }
                        // On event B
                        if (value != null && StringUtils.equalsIgnoreCase(((JSONObject) value).getString("action"), "Logout")) {
                            JSONObject lastAction = (JSONObject) lastA.value();
                            long timestampA = lastAction == null ? 0L : parseTime(lastAction.getString("eventTime"));
                            long timestampB = parseTime(((JSONObject) value).getString("eventTime"));
                            if (timestampB - timestampA > TimeUnit.SECONDS.toMillis(3)) {
                                out.collect(JSONObject.toJSONString(value));
                            }
                        }
                    }
                })
                .uid("Pattern-NotBefore");

        Properties kafkaSinkProps = new Properties();
        kafkaSinkProps.setProperty("bootstrap.servers", "172.16.100.146:9092");
        FlinkKafkaProducer010<String> kafkaSink = new FlinkKafkaProducer010<String>(
                "172.16.100.146:9092",
                "alarm_consecutive_login",
                new SimpleStringSchema()
        );
        kafkaSink.setWriteTimestampToKafka(true);
        windowCount.addSink(kafkaSink);
        notFollowedBy.addSink(kafkaSink);
        repeatUntil.addSink(kafkaSink);
        notBefore.addSink(kafkaSink);
        stream
                .keyBy(data -> ((JSONObject) data).getString("username"))
                .map(JSONObject::toJSONString)
                .addSink(kafkaSink);

        env.execute("HanSight CEP Demo");

    }

    private static long parseTime(String timestamp) {
        ZonedDateTime zdt = ZonedDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"));
        return zdt.toInstant().toEpochMilli();
    }

}
