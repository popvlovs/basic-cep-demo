package com.hansight.dynamicjob.translator;

import java.util.Objects;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/18
 * @description .
 */
public class SourceEntry extends AbstractEntry {
    private SourceType type;

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

    public SourceEntry name(String name) {
        this.name = name;
        return this;
    }

    public SourceEntry prop(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public SourceType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceEntry)) return false;
        SourceEntry that = (SourceEntry) o;
        return type == that.type &&
                Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id, name, properties);
    }

    public enum SourceType {
        KAFKA
    }
}

