package com.hansight.util.condition;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class AndConditionGroup extends ConditionGroup {

    public AndConditionGroup(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean eval(JSONObject element) {
        for (Condition condition : conditions) {
            if (!condition.eval(element)) {
                return false;
            }
        }
        return true;
    }

    public AndConditionGroup and(Condition... conditions) {
        this.conditions.addAll(new ArrayList<>(Arrays.asList(conditions)));
        return this;
    }

    public static AndConditionGroup of(Condition... conditions) {
        CollectionUtils.reverseArray(conditions);
        return new AndConditionGroup(new ArrayList<>(Arrays.asList(conditions)));
    }
}
