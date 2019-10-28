package com.hansight.dynamicjob.translator;

import com.hansight.dynamicjob.tool.MD5Util;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/22
 * @description .
 */
public abstract class AbstractEntry {
    protected String id;
    protected String name;
    protected Map<String, Object> properties = new HashMap<>();

    public AbstractEntry id(String id) {
        this.id = id;
        return this;
    }

    public AbstractEntry name(String name) {
        this.name = name;
        return this;
    }

    public AbstractEntry prop(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public String getId() {
        if (id != null) {
            return id;
        } else if (name != null) {
            return MD5Util.md5(name);
        } else {
            return null;
        }
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
