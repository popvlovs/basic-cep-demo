package com.hansight;

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

public class HavingCountJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsTableEnv
                .connect(
                        new Kafka()
                                .version("0.10")
                                .topic("userActionsV3")
                                .startFromEarliest()
                                .property("bootstrap.servers", "172.16.100.146:9092")
                                .property("group.id", "test-consumer-group")
                )
                .withFormat(
                        new Json()
                                .deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("username", "VARCHAR")
                                .field("region", "VARCHAR")
                                .field("eventId", "VARCHAR")
                                .field("action", "VARCHAR")
                                .field("rowTime", "TIMESTAMP").rowtime(
                                new Rowtime()
                                        .timestampsFromField("eventTime")
                                        .watermarksPeriodicBounded(3000)
                        )
                                .field("procTime", "TIMESTAMP").proctime()
                )
                .inAppendMode()
                .registerTableSource("user_actions");

        Table sqlResult = bsTableEnv.sqlQuery("SELECT\n" +
                "    HOP_START(rowTime, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS start_time,\n" +
                "    HOP_END(rowTime, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS end_time,\n" +
                "    username AS username,\n" +
                "    action   AS action,\n" +
                "    COUNT(*) AS action_count\n" +
                "FROM user_actions\n" +
                "GROUP BY HOP(rowTime, INTERVAL '10' SECOND, INTERVAL '30' SECOND), username, action\n" +
                "HAVING COUNT(*) > 10\n");
        DataStream<Tuple2<Boolean, Row>> resultStream = bsTableEnv.toRetractStream(sqlResult, Row.class);
        resultStream.print();

        bsEnv.execute("Having Count Detection");
    }
}
