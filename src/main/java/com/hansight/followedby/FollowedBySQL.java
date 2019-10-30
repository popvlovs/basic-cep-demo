package com.hansight.followedby;

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

public class FollowedBySQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsTableEnv
                .connect(
                        new Kafka()
                                .version("0.10")
                                .topic("userActionsV4")
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

        Table sqlResult = bsTableEnv.sqlQuery("SELECT *\n" +
                "FROM user_actions\n" +
                "MATCH_RECOGNIZE(\n" +
                "    PARTITION BY username\n" +
                "    ORDER BY rowTime\n" +
                "    MEASURES\n" +
                "       A.rowTime     AS  start_time,\n" +
                "       A.region      AS  login_region,\n" +
                "       A.eventId     AS  start_event_id,\n" +
                "       B.eventId     AS  end_event_id,\n" +
                "       B.region      AS  logout_region\n" +
                "    ONE ROW PER MATCH\n" +
                "    AFTER MATCH SKIP PAST LAST ROW\n" +
                "    PATTERN (PERMUTE(A B)) WITHIN INTERVAL '3' HOUR\n" +
                "    DEFINE\n" +
                "        A AS A.action = 'Login',\n" +
                "        B AS B.action = 'Logout' AND B.region <> A.region\n" +
                ")\n");
        DataStream<Row> resultStream = bsTableEnv.toAppendStream(sqlResult, Row.class);
        resultStream.print();

        bsEnv.execute("Followed By Detection");
    }
}
