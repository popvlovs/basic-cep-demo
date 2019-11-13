package com.hansight.havingcount;

import com.hansight.udfs.BelongFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class HavingCountIBSQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsTableEnv
                .connect(
                        new Kafka()
                                .version("0.10")
                                .topic("hes-sae-group-0")
                                .startFromEarliest()
                                .property("bootstrap.servers", "172.16.100.193:9092")
                                .property("group.id", "flink-consumer-group-0")
                )
                .withFormat(
                        new Json()
                                .schema(Types.ROW_NAMED(
                                        new String[]{"event_name", "src_address", "event_digest", "threat_info", "occur_time"},
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LONG
                                ))
                        // 不要使用deriveSchema，否则不兼容Long型的时间输入
                        /* new Json()
                                .deriveSchema()*/
                )
                .withSchema(
                        new Schema()
                                .field("event_name", Types.STRING)
                                .field("src_address", Types.STRING)
                                .field("event_digest", Types.STRING)
                                .field("threat_info", Types.STRING)
                                .field("row_time", Types.SQL_TIMESTAMP).rowtime(
                                new Rowtime()
                                        .timestampsFromField("occur_time")
                                        .watermarksPeriodicBounded(3000)
                        )
                                .field("proc_time", Types.SQL_TIMESTAMP).proctime()
                )
                .inAppendMode()
                .registerTableSource("events");

        bsTableEnv.registerFunction("belong", new BelongFunction());

        Table sqlResult = bsTableEnv.sqlQuery("SELECT\n" +
                "    HOP_START(row_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS start_time,\n" +
                "    HOP_END(row_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS end_time,\n" +
                "    COUNT(*) AS action_count\n" +
                "FROM events\n" +
                "WHERE \n" +
                "    event_digest = 'nta_alert' AND \n" +
                "    belong(events.src_address, '内网IP') \n" +
                "GROUP BY HOP(row_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND)\n" +
                "HAVING COUNT(*) > 1000\n");
        bsTableEnv.toRetractStream(sqlResult, Row.class).print();

        Table sqlResult2 = bsTableEnv.sqlQuery("SELECT\n" +
                "    HOP_START(row_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS start_time,\n" +
                "    HOP_END(row_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS end_time,\n" +
                "    COUNT(*) AS action_count\n" +
                "FROM events\n" +
                "WHERE \n" +
                "    event_digest = 'nta_alert' AND \n" +
                "    belong(events.src_address, '内网IP') \n" +
                "GROUP BY HOP(row_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND)\n" +
                "HAVING COUNT(*) > 1000\n");
        bsTableEnv.toRetractStream(sqlResult2, Row.class).print();


        bsEnv.execute("Having Count Detection");
    }
}
