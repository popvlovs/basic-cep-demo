package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.inmemorysource.function.PerformanceTestTableSink;
import com.hansight.source.IncreasingFastJsonSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class MultipleSQL {
    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("172.16.106.142",8081,"C:\\projects\\basic-cep-demo\\build\\libs\\basic-cep-demo-0.1-SNAPSHOT-all.jar");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        env.getConfig().enableObjectReuse();
        env.setParallelism(1);

        ParameterTool parameter = ParameterTool.fromArgs(args);
        int jobNum = parameter.getInt("jobNum", 10);
        System.out.println("Job num: " + jobNum);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(100_000_000, 10))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(JSONObject element) {
                        return element.getLongValue("occur_time");
                    }
                });

        DataStream<Tuple5<Integer, String, String, String, Timestamp>> tableStream = stream
                .map(item -> new Tuple5<>(item.getInteger("key"), item.getString("event_name"), item.getString("src_address"), item.getString("dst_address"), new Timestamp(item.getLongValue("occur_time"))))
                .returns(TypeInformation.of(new TypeHint<Tuple5<Integer, String, String, String, Timestamp>>(){}));

        tableEnv.registerDataStream("events", tableStream, "key, event_name, src_address, dst_address, row_time.rowtime");
        tableEnv.registerTableSink("discard-sink", new PerformanceTestTableSink());
        for (int i = 0; i < jobNum; ++i) {
            String sql = "select \n" +
                    " tumble_start(row_time, interval '1' hour) as start_time,\n" +
                    " tumble_end(row_time, interval '1' hour) as end_time,\n" +
                    " src_address, dst_address, count(1) as cnt \n" +
                    "from events \n" +
                    "where event_name = '邮件登陆' \n" +
                    "group by tumble(row_time, interval '1' hour), src_address, dst_address";
            Table sqlResult = tableEnv.sqlQuery(sql);
            //DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(sqlResult, Row.class);
            //resultStream.print();
            sqlResult.insertInto("discard-sink");
        }
        env.execute("Benchmark");
    }
}
