package com.hansight.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/4
 * @description 输出结果序列化工具类
 */
public class OutputUtil {

    public static String asText(Map<String, List<ObjectNode>> patterns) {
        StringBuilder sb = new StringBuilder();
        for (String patternName : patterns.keySet()) {
            sb.append(patternName + ": ");
            List<ObjectNode> events = patterns.get(patternName);
            String vals = events.stream().map(ObjectNode::toString).reduce((a, b) -> a + ',' + b).orElse("");
            sb.append("[" + vals + "] ");
        }
        return sb.toString();
    }
}
