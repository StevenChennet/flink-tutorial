package com.stevenchennet.net.flink191;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class LongToTimestamp extends ScalarFunction implements Serializable {
    public long eval(long ms) {
        /*
        java.sql.Timestamp ts = java.sql.Timestamp.from(Instant.ofEpochMilli(ms));
        return ts;
        */
        return ms;
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP;
    }
}
