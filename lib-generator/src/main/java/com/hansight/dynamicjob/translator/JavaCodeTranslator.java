package com.hansight.dynamicjob.translator;

import com.alibaba.fastjson.JSONObject;
import com.hansight.dynamicjob.tool.FileUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private Set<SinkEntry> sink = new HashSet<>();
    private StringBuilder code = new StringBuilder();
    private int indentation = 0;
    private Pattern placeholderPtn = Pattern.compile("\\{[a-zA-Z\\.\\_\\-]+}");

    private JavaCodeTranslator() {
    }

    public static JavaCodeTranslator builder() {
        return new JavaCodeTranslator();
    }

    public JavaCodeTranslator addSource(SourceEntry sourceInfo) {
        this.source.add(sourceInfo);
        return this;
    }

    public JavaCodeTranslator addTransformation(TransformationEntry transformationEntry) {
        this.transformation.add(transformationEntry);
        return this;
    }

    public JavaCodeTranslator addSink(SinkEntry sinkEntry) {
        this.sink.add(sinkEntry);
        return this;
    }

    public String build() throws Exception {
        // 1. Append env
        addIndent(8);
        initialEnv();

        // 2. Append source
        for (SourceEntry sourceEntry : this.source) {
            switch (sourceEntry.getType()) {
                case KAFKA:
                    handleKafkaSource(sourceEntry);
                    break;
                default:
                    throw new RuntimeException("Unsupported source type: " + JSONObject.toJSONString(sourceEntry));
            }
        }
        appendLine(" ");

        // 3. Append sink
        for (SinkEntry sinkEntry : this.sink) {
            switch (sinkEntry.getType()) {
                case KAFKA:
                    handleKafkaSink(sinkEntry);
                    break;
                default:
                    throw new RuntimeException("Unsupported sink type: " + JSONObject.toJSONString(sinkEntry));
            }
        }
        appendLine(" ");

        // 4. Append transformation
        for (TransformationEntry transformationEntry : this.transformation) {
            switch (transformationEntry.getType()) {
                case FOLLOWED_BY:
                    handleFollowedByTransformation(transformationEntry);
                    break;
                default:
                    throw new RuntimeException("Unsupported transformation type: " + JSONObject.toJSONString(transformationEntry));
            }
        }

        String body = FileUtil.read("template/MainJob.java");
        return format(body, Collections.singletonMap("code", code.toString()));
    }

    private void initialEnv() throws Exception {
        /*
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        */
        String text = FileUtil.read("template/InitEnv.java");
        appendLine(text);
        appendLine(" ");
    }

    private void handleKafkaSource(SourceEntry sourceEntry) throws Exception {
        if (!sourceEntry.getProperties().containsKey("topic")) {
            throw new RuntimeException("Invalid argument of Kafka source " + sourceEntry.getId() + ": null topic");
        }
        if (!sourceEntry.getProperties().containsKey("watermark")) {
            throw new RuntimeException("Invalid argument of Kafka source " + sourceEntry.getId() + ": null watermark");
        }
        if (Objects.isNull(sourceEntry.getId())) {
            throw new RuntimeException("Invalid argument of Kafka source " + sourceEntry.getId() + ": null uid");
        }
        Map<String, Object> args = getEntryArgs(sourceEntry);
        String kafkaSourceSegment = FileUtil.read("template/KafkaSource.java");
        String text = format(kafkaSourceSegment, args);
        appendLine(text);
    }

    private void handleKafkaSink(SinkEntry sink) throws Exception {
        if (!sink.getProperties().containsKey("topic")) {
            throw new RuntimeException("Invalid argument of Kafka sink " + sink.getId() + ": null topic");
        }
        if (Objects.isNull(sink.getId())) {
            throw new RuntimeException("Invalid argument of Kafka sink " + sink.getId() + ": null uid");
        }
        Map<String, Object> args = getEntryArgs(sink);
        String segment = FileUtil.read("template/KafkaSink.java");
        String text = format(segment, args);
        appendLine(text);
    }

    private void handleFollowedByTransformation(TransformationEntry transformationEntry) throws Exception {
        if (Objects.isNull(transformationEntry.getId())) {
            throw new RuntimeException("Invalid argument of Kafka transformation " + transformationEntry.getId() + ": null uid");
        }

        Map<String, Object> args = getEntryArgs(transformationEntry);
        {
            // Within
            if (transformationEntry.getProperties().containsKey("within")) {
                args.put("within", String.format(".within(Time.seconds(%s))", transformationEntry.getProperties().get("within")));
            } else {
                args.put("within", "");
            }
            // Condition A TODO: parse from hql
            args.put("conditionA", "Objects.equals(val.findValue(\"action\").asText(), \"Login\")");
            // Condition B TODO: parse from hql
            args.put("conditionB", "Objects.equals(val.findValue(\"action\").asText(), \"Logout\")");
            // Input stream TODO: parse from hql
            String sourceStream = String.format("source_%s", transformationEntry.getSourceId());
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

            // Add sink
            String sink = String.format("sink_%s", transformationEntry.getSinkId());
            args.put("sink", sink);
            args.put("sinkName", transformationEntry.getSinkName());
        }

        String code = FileUtil.read("template/FollowedByTransformation.java");
        String text = format(code, args);
        appendLine(text);
    }

    private Map<String, Object> getEntryArgs(AbstractEntry entry) {
        Map<String, Object> args = new HashMap<>();
        args.put("uid", entry.getId());
        args.put("name", entry.getName());
        args.putAll(entry.getProperties());
        return args;
    }

    private String format(String text, Map<String, Object> argsMap) {
        for (String key : argsMap.keySet()) {
            String val = argsMap.get(key).toString();
            text = text.replaceAll("\\{" + key + "}", val);
        }
        Matcher matcher =  placeholderPtn.matcher(text);
        while (matcher.find()) {
            String placeholder = matcher.group();
            log.error("Unmatched placeholder {}, {}", placeholder, text);
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