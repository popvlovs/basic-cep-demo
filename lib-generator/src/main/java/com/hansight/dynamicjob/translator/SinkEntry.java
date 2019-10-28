package com.hansight.dynamicjob.translator;

import java.util.Objects;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/21
 * @description .
 */
public class SinkEntry extends AbstractEntry {
    private SinkType type;

    private SinkEntry(SinkType type) {
        this.type = type;
    }

    public static SinkEntry kafka() {
        return new SinkEntry(SinkType.KAFKA);
    }

    public SinkEntry prop(String key, Object val) {
        this.properties.put(key, val);
        return this;
    }

    public SinkEntry id(String id) {
        this.id = id;
        return this;
    }

    public SinkEntry name(String name) {
        this.name = name;
        return this;
    }

    public SinkType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SinkEntry)) return false;
        SinkEntry sinkEntry = (SinkEntry) o;
        return Objects.equals(id, sinkEntry.id) &&
                Objects.equals(name, sinkEntry.name) &&
                type == sinkEntry.type &&
                Objects.equals(properties, sinkEntry.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, properties);
    }

    public enum SinkType {
        KAFKA, ES, STDOUT
    }
}
