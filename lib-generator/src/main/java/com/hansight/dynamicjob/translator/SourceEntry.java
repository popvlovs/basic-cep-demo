package com.hansight.dynamicjob.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/18
 * @description .
 */
public class SourceEntry {
    private SourceType type;
    private String id;
    private Map<String, Object> properties = new HashMap<>();

    private SourceEntry(SourceType type) {
        this.type = type;
    }

    public static SourceEntry of(SourceType type) {
        return new SourceEntry(type);
    }

    public static SourceEntry kafka() {
        return new SourceEntry(SourceType.KAFKA);
    }

    public SourceEntry id(String id) {
        this.id = id;
        return this;
    }

    public SourceEntry prop(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public SourceType getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceEntry)) return false;

        SourceEntry that = (SourceEntry) o;

        return Objects.equals(that.id, id);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    public enum SourceType {
        KAFKA
    }
}

