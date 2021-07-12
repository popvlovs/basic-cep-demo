package com.hansight.inmemorysource.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by sujun on 2019/11/5.
 */
public class PerformanceTestTableSink implements BatchTableSink<Row>, AppendStreamTableSink<Row> {

    private RowTypeInfo rowTypeInfo;

    public PerformanceTestTableSink() {
        rowTypeInfo = new RowTypeInfo(TypeInformation.of(Timestamp.class), TypeInformation.of(Timestamp.class), TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(Long.class));
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.addSink(new DiscardingSink<>()).name("empty");
    }

    @Override
    public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value) throws Exception {
                // System.out.println(value);
            }
        }).name("empty");
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return rowTypeInfo;
    }

    @Override
    public String[] getFieldNames() {
        return rowTypeInfo.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return rowTypeInfo.getFieldTypes();
    }

    @Override
    public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        try {
            dataSet.count();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
