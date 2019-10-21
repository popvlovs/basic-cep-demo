package com.hansight.dynamicjob.translator;

import com.alibaba.fastjson.JSONObject;
import com.hansight.dynamicjob.tool.FileUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/18
 * @description 用于将 SAE 规则翻译成可编译的 Java 代码
 */
public class JavaCodeTranslator {

    private static final Logger log = LoggerFactory.getLogger(JavaCodeTranslator.class);

    private Set<SourceEntry> source = new HashSet<>();
    private Set<TransformationEntry> transformation = new HashSet<>();
    private Set<String> packages = new HashSet<>();
    private StringBuilder code = new StringBuilder();
    private int indentation = 0;

    private JavaCodeTranslator() {
    }

    public static JavaCodeTranslator builder() {
        return new JavaCodeTranslator();
    }

    public JavaCodeTranslator addImport(String... packages) {
        this.packages.addAll(Arrays.asList(packages));
        return this;
    }

    public JavaCodeTranslator addSource(SourceEntry sourceInfo) {
        this.source.add(sourceInfo);
        return this;
    }

    public JavaCodeTranslator addTransformation(TransformationEntry transformationEntry) {
        this.transformation.add(transformationEntry);
        return this;
    }

    public String build() throws Exception {

        // 1. Append imports
        this.packages.forEach(this::appendLine);
        appendLine(" ");

        // 2. Append main class
        appendLine("public class MainJob {");
        appendLine(" ");
        addIndent();

        {
            // 3. Append env
            initialEnv();

            // 4. Append source
            for (SourceEntry sourceEntry : this.source) {
                switch (sourceEntry.getType()) {
                    case KAFKA:
                        dealKafkaSource(sourceEntry);
                        break;
                    default:
                        throw new RuntimeException("Unsupported source type: " + JSONObject.toJSONString(sourceEntry));
                }
            }
            appendLine(" ");

            // 5. Append transformation
            for (TransformationEntry transformationEntry : this.transformation) {
                switch (transformationEntry.getType()) {
                    case FOLLOWED_BY:
                        dealFollowedByTransformation(transformationEntry);
                        break;
                    default:
                        throw new RuntimeException("Unsupported transformation type: " + JSONObject.toJSONString(transformationEntry));
                }
            }
            appendLine(" ");
        }

        // 2. Append main class end
        decIndent();
        appendLine("}");

        return code.toString();
    }

    private void initialEnv() {
        /*
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        */
        appendLine("final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        appendLine("env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);");
    }

    private void dealKafkaSource(SourceEntry sourceEntry) throws Exception {
        if (!sourceEntry.getProperties().containsKey("topic")) {
            throw new RuntimeException("Invalid argument of Kafka source " + sourceEntry.getId() + ": null topic");
        }
        if (!sourceEntry.getProperties().containsKey("watermark")) {
            throw new RuntimeException("Invalid argument of Kafka source " + sourceEntry.getId() + ": null watermark");
        }
        if (Objects.isNull(sourceEntry.getId())) {
            throw new RuntimeException("Invalid argument of Kafka source " + sourceEntry.getId() + ": null uid");
        }
        // Topic
        Map<String, Object> args = new HashMap<>();
        args.put("topic", sourceEntry.getProperties().get("topic").toString());
        sourceEntry.getProperties().remove("topic");
        // Watermark
        args.put("watermark", MapUtils.getInteger(sourceEntry.getProperties(), "watermark", 3000));
        sourceEntry.getProperties().remove("watermark");
        // Props
        StringBuilder props = new StringBuilder();
        sourceEntry.getProperties().forEach((key, value) -> props.append(String.format("properties.setProperty(\"%s\", \"%s\");\n", key, value)));
        args.put("props", props.toString());
        // UID
        args.put("uid", sourceEntry.getId());

        String kafkaSourceSegment = FileUtil.read("template/KafkaSource.java");
        String text = format(kafkaSourceSegment, args);
        appendLine(text);
    }

    private void dealFollowedByTransformation(TransformationEntry transformationEntry) throws Exception {
        if (Objects.isNull(transformationEntry.getId())) {
            throw new RuntimeException("Invalid argument of Kafka transformation " + transformationEntry.getId() + ": null uid");
        }

        // Within
        Map<String, Object> args = new HashMap<>();
        if (transformationEntry.getProperties().containsKey("within")) {
            args.put("within", String.format(".within(Time.seconds(%s)", transformationEntry.getProperties().get("within")));
        } else {
            args.put("within", "");
        }
        // UID
        args.put("uid", transformationEntry.getId());
        // Condition A TODO: parse from hql
        args.put("conditionA", "Objects.equals(val.findValue(\"action\").asText(), \"Login\")");
        // Condition B TODO: parse from hql
        args.put("conditionB", "Objects.equals(val.findValue(\"action\").asText(), \"Logout\")");
        // Input stream TODO: parse from hql
        String sourceStream = String.format("stream_%s", transformationEntry.getSourceId());
        args.put("input", String.format("%s.keyBy(data -> data.findValue(\"username\"))", sourceStream));
        // Measure
        args.put("measure", "StringBuilder sb = new StringBuilder();\n" +
                "                    for (String patternName : matchedEvents.keySet()) {\n" +
                "                        sb.append(patternName + \": \");\n" +
                "                        List<ObjectNode> events = matchedEvents.get(patternName);\n" +
                "                        String vals = events.stream().map(ObjectNode::toString).reduce((a, b) -> a + ',' + b).orElse(\"\");\n" +
                "                        sb.append(\"[\" + vals + \"] \");\n" +
                "                    }\n" +
                "                    return sb.toString();");

        String code = FileUtil.read("template/FollowedByTransformation.java");
        String text = format(code, args);
        appendLine(text);
    }

    private String format(String text, Map<String, Object> argsMap) {
        for (String key : argsMap.keySet()) {
            String val = argsMap.get(key).toString();
            text = text.replaceAll("\\{" + key + "}", val);
        }
        return text;
    }

    private void appendLine(String text, Object... args) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(text.getBytes(Charset.forName("utf8"))), Charset.forName("utf8")));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                code.append(StringUtils.repeat(' ', indentation));
                code.append(String.format(line, args));
                code.append("\n");
            }
        } catch (Exception e) {
            log.error("Error on append line: {}", text, e);
        }
    }

    private void addIndent() {
        addIndent(4);
    }

    private void addIndent(int num) {
        indentation += num;
    }

    private void decIndent() {
        decIndent(4);
    }

    private void decIndent(int num) {
        indentation = Integer.max(indentation - num, 0);
    }
}