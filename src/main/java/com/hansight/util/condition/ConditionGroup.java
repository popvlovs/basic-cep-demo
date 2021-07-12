package com.hansight.util.condition;

import java.util.List;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public abstract class ConditionGroup extends Condition {
    /**
     * Sub conditions
     */
    protected List<Condition> conditions;
}
