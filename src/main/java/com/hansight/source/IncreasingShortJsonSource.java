package com.hansight.source;

import com.hansight.util.MiscUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/20
 * @description .
 */
public class IncreasingShortJsonSource extends BaseSourceWithKeyRange<ObjectNode> {

    private static final long serialVersionUID = 2941333602938145526L;

    public IncreasingShortJsonSource(int numEvents, int numKeys) {
        super(numEvents, numKeys);
    }

    @Override
    protected ObjectNode getElement(int keyId) {
        try {
            return MiscUtil.getObjectNodeFromJson("{\"occur_time\":" + System.currentTimeMillis() + ",\"partition\": \"" + keyId + "\"}", "occur_time");
        } catch (Exception e) {
            return null;
        }
    }
}
