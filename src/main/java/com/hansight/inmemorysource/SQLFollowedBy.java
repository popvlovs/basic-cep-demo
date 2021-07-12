package com.hansight.inmemorysource;

import com.alibaba.fastjson.JSONObject;
import com.hansight.DiscardingSink;
import com.hansight.source.IncreasingFastJsonSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class SQLFollowedBy {

    /**
     * There is something wrong with this job
     * 尝试提交SQL任务，但是报KeyGroup错误，这个错误在Blink环境无法复现，怀疑是Flink的bug
     *
     * Blink集群下有相同的现象？
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /**
         * 实测中发现，使用blink planner会导致key group range运行时异常，导致任务终止
         * 所以这里换成old planner
         */
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStream<JSONObject> stream = env.addSource(new IncreasingFastJsonSource(15_000_000, 10))
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
        Table sqlResult = tableEnv.sqlQuery("select * from events MATCH_RECOGNIZE(PARTITION BY key ORDER BY row_time MEASURES A.row_time as start_time, A.event_name as start_event_name, B.row_time as end_time, B.event_name as end_event_name ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B) WITHIN INTERVAL '30' MINUTE DEFINE A as A.event_name = '邮件登陆', B as B.event_name = '邮件发送')");
        DataStream<Row> resultStream = tableEnv.toAppendStream(sqlResult, Row.class);

        resultStream.print();

        env.execute("Benchmark");
    }
}
