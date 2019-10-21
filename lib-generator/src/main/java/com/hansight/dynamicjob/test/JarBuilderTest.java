package com.hansight.dynamicjob.test;

import com.alibaba.fastjson.JSONObject;
import com.hansight.dynamicjob.generator.JarBuilder;
import com.hansight.dynamicjob.tool.FileUtil;
import com.hansight.dynamicjob.tool.FlinkRestApiUtil;
import com.hansight.dynamicjob.tool.MD5Util;
import com.hansight.dynamicjob.translator.JavaCodeTranslator;
import com.hansight.dynamicjob.translator.SourceEntry;
import com.hansight.dynamicjob.translator.TransformationEntry;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/16
 * @description .
 */
public class JarBuilderTest {

    public static void main(String[] args) {
        try {
            // 0. Translate rule to Java code
            String kafkaSourceId = MD5Util.md5("KAFKA数据源");
            String followedByTransId = MD5Util.md5("FollowedBy测试规则#1");
            String javaCode = JavaCodeTranslator.builder()
                    .addImport(FileUtil.read("template/HeaderImport.java"))
                    .addSource(SourceEntry.kafka()
                            .id(kafkaSourceId)
                            .prop("bootstrap.servers", "172.16.100.146:9092")
                            .prop("group.id", "kafka-event-source-group")
                            .prop("topic", "userActionsV3")
                            .prop("watermark", 3000))
                    .addTransformation(TransformationEntry.followedBy()
                            .id(followedByTransId)
                            .source("kafka_source_test"))
                    .build();

            System.out.println(javaCode);

            // 1. Rebuild Jar
            File outJar = JarBuilder.builder()
                    .workspace("C:\\Users\\yitian_song\\Desktop\\JarOutput")
                    .sourceJar("build/basic-cep-demo-0.1-SNAPSHOT-all.jar")
                    .dependencyJarDir("build/lib")
                    .mainClassToCompile(javaCode)
                    .compile();
            System.out.println("Compile Jar file success: " + outJar.getCanonicalPath());

            // 2. Request savepoint and cancel job
            String url = "http://172.16.100.146:8081";
            String jobId = "78350269e2c62c139da38019d0d0a780";
            String savepointDir = "/opt/hansight/flink/flink-1.9.0/savepoint/";
            String requestId = FlinkRestApiUtil.savepointAsync(url, jobId, savepointDir, true);
            System.out.println("Cancel Job " + jobId + " success, savepoint request accepted: " + requestId);

            // 3. Waiting for savepoint complete
            FlinkRestApiUtil.waitSavepointComplete(url, jobId, requestId, TimeUnit.MINUTES.toMillis(5));

            // 4. Get complete savepoint
            String savepoint = FlinkRestApiUtil.getCompleteCheckpointRetry(url, jobId, 3);
            System.out.println("Complete savepoint: " + savepoint);

            // 5. Upload new Jar
            String jarId = FlinkRestApiUtil.uploadJar(url, outJar);
            System.out.println("Upload Jar success: " + jarId);

            // 6. Delete older Jars
            List<JSONObject> uploadedJars = FlinkRestApiUtil.getUploadedJars(url, name -> name.contains(outJar.getName()));
            int topN = 3;
            if (uploadedJars.size() > topN) {
                uploadedJars.subList(topN, uploadedJars.size()).forEach(jar -> FlinkRestApiUtil.deleteJar(url, jar.getString("id")));
            }

            // 7. Run new Jar with savepoint
            String mainClass = JarBuilder.builder().getMainClassFromJar(outJar);
            String runJobId = FlinkRestApiUtil.runJar(url, jarId, mainClass, 1, savepoint);
            System.out.println("Restore job: " + runJobId + " from savepoint " + savepoint);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getJavaCode() {
        return "package com.hansight;\n" +
                "\n" +
                "import org.apache.flink.streaming.api.TimeCharacteristic;\n" +
                "import org.apache.flink.streaming.api.datastream.DataStream;\n" +
                "import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;\n" +
                "import org.apache.flink.table.api.EnvironmentSettings;\n" +
                "import org.apache.flink.table.api.Table;\n" +
                "import org.apache.flink.table.api.java.StreamTableEnvironment;\n" +
                "import org.apache.flink.table.descriptors.Json;\n" +
                "import org.apache.flink.table.descriptors.Kafka;\n" +
                "import org.apache.flink.table.descriptors.Rowtime;\n" +
                "import org.apache.flink.table.descriptors.Schema;\n" +
                "import org.apache.flink.types.Row;\n" +
                "\n" +
                "public class TableJob  {\n" +
                "\n" +
                "    public static void main(String[] args) throws Exception {\n" +
                "\n" +
                "        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();\n" +
                "        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);\n" +
                "        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();\n" +
                "        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);\n" +
                "\n" +
                "        bsTableEnv\n" +
                "                .connect(\n" +
                "                        new Kafka()\n" +
                "                                .version(\"0.10\")\n" +
                "                                .topic(\"userActionsV3\")\n" +
                "                                .startFromEarliest()\n" +
                "                                .property(\"bootstrap.servers\", \"172.16.100.146:9092\")\n" +
                "                                .property(\"group.id\", \"test-consumer-group\")\n" +
                "                )\n" +
                "                .withFormat(\n" +
                "                        new Json()\n" +
                "                                .deriveSchema()\n" +
                "                )\n" +
                "                .withSchema(\n" +
                "                        new Schema()\n" +
                "                                .field(\"username\", \"VARCHAR\")\n" +
                "                                .field(\"region\", \"VARCHAR\")\n" +
                "                                .field(\"eventId\", \"VARCHAR\")\n" +
                "                                .field(\"action\", \"VARCHAR\")\n" +
                "                                .field(\"rowTime\", \"TIMESTAMP\").rowtime(\n" +
                "                                new Rowtime()\n" +
                "                                        .timestampsFromField(\"eventTime\")\n" +
                "                                        .watermarksPeriodicBounded(3000)\n" +
                "                        )\n" +
                "                                .field(\"procTime\", \"TIMESTAMP\").proctime()\n" +
                "                )\n" +
                "                .inAppendMode()\n" +
                "                .registerTableSource(\"user_actions\");\n" +
                "\n" +
                "        Table sqlResult = bsTableEnv.sqlQuery(\"SELECT *\\n\" +\n" +
                "                \"FROM user_actions\\n\" +\n" +
                "                \"MATCH_RECOGNIZE(\\n\" +\n" +
                "                \"    PARTITION BY username\\n\" +\n" +
                "                \"    ORDER BY rowTime\\n\" +\n" +
                "                \"    MEASURES\\n\" +\n" +
                "                \"       A.rowTime     AS  start_time,\\n\" +\n" +
                "                \"       A.region      AS  login_region,\\n\" +\n" +
                "                \"       A.eventId     AS  start_event_id,\\n\" +\n" +
                "                \"       B.eventId     AS  end_event_id,\\n\" +\n" +
                "                \"       B.region      AS  logout_region\\n\" +\n" +
                "                \"    ONE ROW PER MATCH\\n\" +\n" +
                "                \"    AFTER MATCH SKIP PAST LAST ROW\\n\" +\n" +
                "                \"    PATTERN (A B) WITHIN INTERVAL '3' HOUR\\n\" +\n" +
                "                \"    DEFINE\\n\" +\n" +
                "                \"        A AS A.action = 'Login',\\n\" +\n" +
                "                \"        B AS B.action = 'Logout' AND B.region <> A.region\\n\" +\n" +
                "                \")\\n\");\n" +
                "        DataStream<Row> resultStream = bsTableEnv.toAppendStream(sqlResult, Row.class);\n" +
                "        resultStream.print();\n" +
                "\n" +
                "        bsEnv.execute(\"Followed By Detection\");\n" +
                "    }\n" +
                "}\n";
    }
}
