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
public class OrConditionGroup extends ConditionGroup {

    public OrConditionGroup(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean eval(JSONObject element) {
        for (Condition condition : conditions) {
            if (condition.eval(element)) {
                return true;
            }
        }
        return false;
    }

    public OrConditionGroup or(Condition... conditions) {
        this.conditions.addAll(new ArrayList<>(Arrays.asList(conditions)));
        return this;
    }

    public static OrConditionGroup of(Condition... conditions) {
        CollectionUtils.reverseArray(conditions);
        return new OrConditionGroup(new ArrayList<>(Arrays.asList(conditions)));
    }
}
