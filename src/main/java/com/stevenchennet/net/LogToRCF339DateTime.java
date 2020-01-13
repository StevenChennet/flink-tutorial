package com.stevenchennet.net;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class LogToRCF339DateTime extends ScalarFunction implements Serializable {
    public java.sql.Timestamp eval(long ms) {
        ZoneId asiaZone = ZoneId.of("Asia/Shanghai");
        //ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ms), asiaZone);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(new java.sql.Timestamp(ms).toInstant(), asiaZone);



        DateTimeFormatter dtf = DateTimeFormatter.ISO_ZONED_DATE_TIME;
        String timeStr = dtf.format(zdt);


        java.sql.Timestamp ts = java.sql.Timestamp.from(Instant.ofEpochMilli(ms));
        return ts;
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP;
    }
}