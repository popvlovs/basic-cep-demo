package com.hansight.source;

import com.alibaba.fastjson.JSONObject;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/20
 * @description .
 */
public class IncreasingFastJsonMiniSource extends BaseSourceWithKeyRange<JSONObject> {

    private static final long serialVersionUID = 2941333602938145526L;

    public IncreasingFastJsonMiniSource(int numEvents, int numKeys) {
        super(numEvents, numKeys);
    }

    @Override
    protected JSONObject getElement(int keyId) {
        try {
            JSONObject element = new JSONObject();
            element.put("key", keyId);
            element.put("event_name", "DNS查询");
            element.put("cnt", 1);
            return element;
        } catch (Exception e) {
            return null;
        }
    }
}
