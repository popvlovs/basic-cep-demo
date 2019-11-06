package com.hansight.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/25
 * @description 计算表达式boolean的工具类
 */
public class ExpressionUtil {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionUtil.class);

    public static boolean equal(ObjectNode objA, String fieldA, ObjectNode objB, String fieldB) {
        validate(objA, fieldA, objB, fieldB);
        JsonNode nodeA = objA.findValue(fieldA);
        if (nodeA == null) {
            return false;
        }
        JsonNode nodeB = objB.findValue(fieldB);
        if (nodeB == null) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(nodeA.asText(), nodeB.asText());
    }

    public static boolean equal(ObjectNode data, String field, Object value) {
        validate(data, field, value);
        JsonNode node = data.findValue(field);
        if (node == null) {
            return false;
        }
        switch (node.getNodeType()) {
            case STRING:
            case BOOLEAN:
            case OBJECT:
                return Objects.equals(node.asText(), value.toString());
            case POJO:
            case NULL:
            case MISSING:
            case ARRAY:
                return false;
            default:
                return Objects.equals(node.asText(), value.toString());
        }
    }

    public static boolean notEqual(ObjectNode data, String field, String value) {
        return !equal(data, field, value);
    }

    public static boolean gt(ObjectNode data, String field, double value) {
        validate(data, field, value);
        Double nodeVal = getFieldAsValue(data, field);
        if (nodeVal == null) {
            return false;
        }
        return Double.compare(nodeVal, value) > 0;
    }

    public static boolean gte(ObjectNode data, String field, double value) {
        validate(data, field, value);
        Double nodeVal = getFieldAsValue(data, field);
        if (nodeVal == null) {
            return false;
        }
        return Double.compare(nodeVal, value) >= 0;
    }

    public static boolean lt(ObjectNode data, String field, double value) {
        validate(data, field, value);
        Double nodeVal = getFieldAsValue(data, field);
        if (nodeVal == null) {
            return false;
        }
        return Double.compare(nodeVal, value) < 0;
    }

    public static boolean lte(ObjectNode data, String field, double value) {
        validate(data, field, value);
        Double nodeVal = getFieldAsValue(data, field);
        if (nodeVal == null) {
            return false;
        }
        return Double.compare(nodeVal, value) <= 0;
    }

    public static boolean exist(ObjectNode data, String field) {
        validate(data, field);
        JsonNode node = data.findValue(field);
        return node == null;
    }

    public static boolean notExist(ObjectNode data, String field) {
        return !exist(data, field);
    }

    public static boolean like(ObjectNode data, String field, String value) {
        validate(data, field, value);
        String fieldAsText = getFieldAsText(data, field);
        if (fieldAsText == null) {
            return false;
        }
        return fieldAsText.contains(value);
    }

    public static boolean rlike(ObjectNode data, String field, String value) {
        validate(data, field, value);
        Pattern pattern = Pattern.compile(value);
        String fieldAsText = getFieldAsText(data, field);
        if (fieldAsText == null) {
            return false;
        }
        return pattern.matcher(fieldAsText).matches();
    }

    public static boolean in(ObjectNode data, String field, String value) {
        validate(data, field, value);
        String fieldAsText = getFieldAsText(data, field);
        IntelligenceGroupUtil.IpRangeMatcher matcher = new IntelligenceGroupUtil.IpRangeMatcher(value);
        return matcher.match(fieldAsText);
    }

    public static boolean contain(ObjectNode data, String field, Object value) {
        validate(data, field, value);
        Iterator<JsonNode> it = getFieldAsArray(data, field);
        while(it != null && it.hasNext()) {
            JsonNode node = it.next();
            boolean contains = false;
            if (node instanceof IntNode) {
                contains = Objects.equals(node.intValue(), value);
            }
            if (node instanceof LongNode) {
                contains = Objects.equals(node.longValue(), value);
            }
            if (node instanceof DoubleNode) {
                contains = Objects.equals(node.doubleValue(), value);
            }
            if (node instanceof FloatNode) {
                contains = Objects.equals(node.floatValue(), value);
            }
            if (contains) {
                return true;
            }
        }
        return false;
    }

    public static boolean match(ObjectNode data, String field) {
        validate(data, field);
        String iocMatcher = getFieldAsText(data, "ioc_matcher");
        return StringUtils.equals(iocMatcher, field);
    }

    public static boolean belong(ObjectNode data, String field, String value) {
        validate(data, field, value);
        String nodeVal = getFieldAsText(data, field);
        return IntelligenceGroupUtil.contains(value, nodeVal);
    }

    private static void validate(Object... inputs) {
        for (Object input : inputs) {
            if (input == null) {
                throw new IllegalArgumentException("Null argument: [" + Arrays.stream(inputs)
                        .map(item -> Optional.ofNullable(item).orElse("null").toString())
                        .reduce((a, b) -> a + ", " + b)+ "]");
            }
        }
    }

    public static String getFieldAsText(ObjectNode data, String field, String defaultVal) {
        JsonNode node = data.findValue(field);
        if (node == null) {
            return defaultVal;
        }
        return node.asText(defaultVal);
    }

    public static String getFieldAsText(ObjectNode data, String field) {
        JsonNode node = data.findValue(field);
        if (node == null) {
            return null;
        }
        return node.asText();
    }

    public static Tuple getFieldsAsText(ObjectNode val, String... fields) {
        int fieldNum = fields.length;
        Tuple tuple = Tuple.newInstance(fieldNum);
        for (int i = 0; i < fields.length; i++) {
            tuple.setField(getFieldAsText(val, fields[i], "None"), i);
        }
        return tuple;
    }

    public static Double getFieldAsValue(ObjectNode data, String field, Double defaultVal) {
        JsonNode node = data.findValue(field);
        if (node == null) {
            return defaultVal;
        }
        return node.asDouble(defaultVal);
    }

    public static Double getFieldAsValue(ObjectNode data, String field) {
        JsonNode node = data.findValue(field);
        if (node == null) {
            return null;
        }
        if (node instanceof IntNode) {
            return node.doubleValue();
        }
        if (node instanceof LongNode) {
            return node.doubleValue();
        }
        if (node instanceof DoubleNode) {
            return node.doubleValue();
        }
        if (node instanceof FloatNode) {
            return node.doubleValue();
        }
        return null;
    }

    private static Iterator<JsonNode> getFieldAsArray(ObjectNode data, String field) {
        JsonNode node = data.findValue(field);
        if (node == null) {
            return null;
        }
        if (node.isArray()) {
            return node.elements();
        }
        return null;
    }

    public static Long getFieldAsTimestamp(ObjectNode data, String field) {
        JsonNode node = data.findValue(field);
        if (node == null) {
            return 0L;
        }
        return node.asLong(0L);
    }

    public static String getGroupSignature(ObjectNode data, String... fields) {
        if (fields.length == 0) {
            return "All";
        }
        return Stream.of(fields)
                .map(field -> getFieldAsText(data, field, "None"))
                .reduce((a, b) -> a + "," + b)
                .orElse("None");
    }
}
