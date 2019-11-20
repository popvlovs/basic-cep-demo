package com.hansight;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/18
 * @description .
 */
public class FastJsonSchema implements DeserializationSchema<JSONObject>, SerializationSchema<JSONObject> {

    @Override
    public JSONObject deserialize(byte[] message) throws IOException {
        return JSONObject.parseObject(new String(message, Charset.forName("UTF-8")));
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(JSONObject element) {
        return new byte[0];
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return null;
    }
}
