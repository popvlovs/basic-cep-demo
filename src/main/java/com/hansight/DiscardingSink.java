package com.hansight;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DiscardingSink<T> implements SinkFunction<T> {
    private static final long serialVersionUID = 1L;

    public DiscardingSink() {
    }

    public void invoke(T value) {
    }
}
