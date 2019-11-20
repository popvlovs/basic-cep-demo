package com.hansight.repeatuntil;

import com.hansight.udfs.BelongFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class RepeatUntilDDL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        /*
        CREATE TABLE events_ddl(
          `event_name` VARCHAR,
          `src_address` VARCHAR,
          `dst_address` VARCHAR,
          `event_digest` VARCHAR,
          `threat_info` VARCHAR,
          `occur_time` TIMESTAMP
        ) WITH (
          'connector.type' = 'kafka',
          'connector.version' = '0.10',
          'connector.topic' = 'hes-sae-group-0',
          'update-mode' = 'append',
          'connector.properties.0.key' = 'zookeeper.connect',
          'connector.properties.0.value' = '172.16.100.193:2181',
          'connector.properties.1.key' = 'bootstrap.servers',
          'connector.properties.1.value' = '172.16.100.193:9092',
          'connector.properties.2.key' = 'group.id',
          'connector.properties.2.value' = 'testGroup',
          'connector.startup-mode' = 'earliest-offset',
          'format.type' = 'json',
          'format.fail-on-missing-field' = 'false',
          'format.json-schema' = '{"type": "object", "properties": {"event_name": {"type": "string"}, "src_address": {"type": "string"}, "dst_address": {"type": "string"}, "event_digest": {"type": "string"}, "threat_info": {"type": "string"}, "occur_time": {"type": "number"}}}'
        );
         */
        String createTable = "CREATE TABLE events_ddl(\n" +
                "          `event_name` VARCHAR,\n" +
                "          `src_address` VARCHAR,\n" +
                "          `dst_address` VARCHAR,\n" +
                "          `event_digest` VARCHAR,\n" +
                "          `threat_info` VARCHAR,\n" +
                "          `occur_time` TIMESTAMP\n" +
                "        ) WITH (\n" +
                "          'connector.type' = 'kafka',\n" +
                "          'connector.version' = '0.10',\n" +
                "          'connector.topic' = 'hes-sae-group-0',\n" +
                "          'update-mode' = 'append',\n" +
                "          'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "          'connector.properties.0.value' = '172.16.100.193:2181',\n" +
                "          'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "          'connector.properties.1.value' = '172.16.100.193:9092',\n" +
                "          'connector.properties.2.key' = 'group.id',\n" +
                "          'connector.properties.2.value' = 'testGroup',\n" +
                "          'connector.startup-mode' = 'earliest-offset',\n" +
                "          'format.type' = 'json',\n" +
                "          'format.fail-on-missing-field' = 'false',\n" +
                "          'format.derive-schema' = 'true'\n" +
                "        )";
        bsTableEnv.sqlUpdate(createTable);

        bsTableEnv.registerFunction("belong", new BelongFunction());

        addCepSqlQuery(bsTableEnv);

        bsEnv.execute("Repeat Until DDL");
    }

    private static void addCepSqlQuery(StreamTableEnvironment bsTableEnv) {
        Table sqlResult = bsTableEnv.sqlQuery("SELECT *\n" +
                "FROM events_ddl\n" +
                "  MATCH_RECOGNIZE (\n" +
                "  PARTITION BY event_name\n" +
                "  ORDER BY occur_time\n" +
                "  MEASURES \n" +
                "    FIRST(A.src_address) as a_src,\n" +
                "    FIRST(B.src_address) as b_src,\n" +
                "    FIRST(A.dst_address) as a_dst,\n" +
                "    FIRST(B.dst_address) as b_dst\n" +
                "  ONE ROW PER MATCH\n" +
                "  AFTER MATCH SKIP PAST LAST ROW\n" +
                "  PATTERN (A{3,} B) WITHIN INTERVAL '1' MINUTE\n" +
                "  DEFINE\n" +
                "    A as A.src_address = '127.0.0.1',\n" +
                "    B as B.src_address <> '127.0.0.1'\n" +
                "  ) AS T");
        bsTableEnv.toAppendStream(sqlResult, Row.class).print();
    }
}
