package com.hansight.dynamicjob.translator;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/21
 * @description .
 */
public class SinkEntry {
    private String id;
    private SinkType type;
    private Map<String, Object> properties = new HashMap<>();

    private SinkEntry(SinkType type) {
        this.type = type;
    }

    public static SinkEntry kafka() {
        return new SinkEntry(SinkType.KAFKA);
    }

    public enum SinkType {
        KAFKA, ES, STDOUT
    }
}
