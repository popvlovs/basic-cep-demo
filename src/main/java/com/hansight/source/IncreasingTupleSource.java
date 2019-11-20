package com.hansight.source;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/20
 * @description .
 */
public class IncreasingTupleSource extends BaseSourceWithKeyRange<Tuple2<Integer, Integer>> {

    private static final long serialVersionUID = 2941333602938145526L;

    public IncreasingTupleSource(int numEvents, int numKeys) {
        super(numEvents, numKeys);
    }

    @Override
    protected Tuple2<Integer, Integer> getElement(int keyId) {
        return new Tuple2<>(keyId, 1);
    }
}
