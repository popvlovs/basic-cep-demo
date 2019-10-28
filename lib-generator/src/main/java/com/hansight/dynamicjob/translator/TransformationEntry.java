package com.hansight.dynamicjob.translator;

import com.hansight.dynamicjob.sae.RuleRawEntity;
import com.hansight.dynamicjob.translator.rule.FollowedBy;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/21
 * @description .
 */
public class TransformationEntry extends AbstractEntry {
    protected TransformationType type;
    protected SourceEntry source;
    protected SinkEntry sink;
    protected RuleRawEntity rule;

    public TransformationEntry(TransformationType type) {
        this.type = type;
    }

    public static TransformationEntry followedBy() {
        return new FollowedBy();
    }

    public TransformationEntry id(String id) {
        this.id = id;
        return this;
    }

    public TransformationEntry name(String name) {
        this.name = name;
        return this;
    }

    public TransformationEntry prop(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public TransformationEntry source(SourceEntry source) {
        this.source = source;
        return this;
    }

    public TransformationEntry sink(SinkEntry sink) {
        this.sink = sink;
        return this;
    }

    public TransformationEntry raw(RuleRawEntity rule) {
        this.rule = rule;
        return this;
    }

    public TransformationType getType() {
        return type;
    }

    public String getSourceId() {
        return source.getId();
    }

    public String getSinkId() {
        return sink.getId();
    }

    public String getSinkName() {
        return sink.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof TransformationEntry)) return false;

        TransformationEntry that = (TransformationEntry) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(that))
                .append(type, that.type)
                .append(source, that.source)
                .append(sink, that.sink)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(type)
                .append(source)
                .append(sink)
                .toHashCode();
    }

    public enum TransformationType {
        FOLLOWED_BY
    }
}
