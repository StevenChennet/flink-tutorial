package com.stevenchennet.net;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka08TableSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import java.util.stream.Stream;

public class FlinkApp {
    public static void main(String[] args) {
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig);

        StreamTableEnvironment ste = StreamTableEnvironment.create(env);


    }

    private static void testA(StreamExecutionEnvironment env, StreamTableEnvironment ste) {
        TableSchema tableSchema = TableSchema.builder()
                .field("UptTimeMs", DataTypes.BIGINT())
                .field("MinutePower", DataTypes.DOUBLE())
                .build();

        JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema
                .Builder(tableSchema.toRowType())
                .build();

        Kafka08TableSource source = new Kafka08TableSource()
    }
}
