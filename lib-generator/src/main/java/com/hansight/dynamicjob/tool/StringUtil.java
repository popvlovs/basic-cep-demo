package com.hansight.dynamicjob.tool;


import org.apache.commons.lang3.StringUtils;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/8/7
 * @description .
 */
public class StringUtil {

    static public boolean equalsIgnoreCaseAndEmpty(String lhs, String rhs) {
        return (lhs == rhs)
                || (lhs == null && StringUtils.isEmpty(rhs))
                || (rhs == null && StringUtils.isEmpty(lhs))
                || (StringUtils.equalsIgnoreCase(lhs, rhs));
    }
}
