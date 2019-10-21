package com.hansight.dynamicjob.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/21
 * @description .
 */
public class TransformationEntry {
    private TransformationType type;
    private Map<String, Object> properties = new HashMap<>();
    private String sourceId;
    private String id;

    public TransformationEntry(TransformationType type) {
        this.type = type;
    }

    public static TransformationEntry followedBy() {
        return new TransformationEntry(TransformationType.FOLLOWED_BY);
    }

    public TransformationEntry id(String id) {
        this.id = id;
        return this;
    }

    public TransformationEntry source(String id) {
        this.sourceId = id;
        return this;
    }

    public TransformationEntry prop(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }

    public TransformationType getType() {
        return type;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransformationEntry)) return false;
        TransformationEntry that = (TransformationEntry) o;
        return type == that.type &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(sourceId, that.sourceId) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, properties, sourceId, id);
    }

    public enum TransformationType {
        FOLLOWED_BY
    }
}
