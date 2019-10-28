package com.hansight.dynamicjob.test;

import com.alibaba.fastjson.JSONObject;
import com.hansight.dynamicjob.generator.JarBuilder;
import com.hansight.dynamicjob.tool.FlinkRestApiUtil;
import com.hansight.dynamicjob.translator.JavaCodeTranslator;
import com.hansight.dynamicjob.translator.SinkEntry;
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
            SourceEntry source;
            SinkEntry sink;
            String javaCode = JavaCodeTranslator.builder()
                    .addSource((source = SourceEntry.kafka()
                            .name("Kafka Source")
                            .prop("bootstrap.servers", "172.16.100.146:9092")
                            .prop("group.id", "event-consumer-group")
                            .prop("topic", "event")
                            .prop("eventTime.field", "eventTime")
                            .prop("eventTime.format", "yyyy-MM-dd'T'HH:mm:ss.SSSz")
                            .prop("watermark", 3000)))
                    .addSink((sink = SinkEntry.kafka()
                            .name("Kafka Sink")
                            .prop("bootstrap.servers", "172.16.100.146:9092")
                            .prop("topic", "alarm")))
                    .addTransformation(TransformationEntry.followedBy()
                            .name("FollowedBy测试规则#3")
                            .source(source)
                            .sink(sink)
                            .prop("within", 3))
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
            String jobId = "3f33c4c836792944e4f2152f640b0f32";
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
}
