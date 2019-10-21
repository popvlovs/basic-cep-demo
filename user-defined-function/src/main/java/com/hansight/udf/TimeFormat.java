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
public class TimeFormat extends ScalarFunction {

    public String eval(Timestamp date) {
        ZonedDateTime ldt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
    }

}
