package com.hansight.util.condition;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public abstract class Condition extends AbstractCondition<JSONObject> implements Serializable {

    protected boolean not = false;

    public Condition reverse() {
        this.not = !not;
        return this;
    }
}
