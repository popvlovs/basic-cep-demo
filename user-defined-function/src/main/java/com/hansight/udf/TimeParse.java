package com.hansight.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/9/12
 * @description .
 */
public class TimeParse extends ScalarFunction {

    public Long eval(Timestamp date) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return zdt.toInstant().toEpochMilli();
    }

}
