package com.stevenchennet.net;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka08TableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

public class FlinkApp {
    public static void main(String[] args){
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig);

        StreamTableEnvironment ste = StreamTableEnvironment.create(env);


    }

    private static void FunC(StreamTableEnvironment ste) throws Exception {
        ste.registerFunction("AAA", new LogToRCF339DateTime());
        Kafka kafka08 = new Kafka().version("0.8").topic("BDP-ChargingMinuteMetric").startFromEarliest().properties(getKafkaProperties());
        Schema schema = new Schema()
                .field("UptTimeMs", Types.LONG)
                .field("BillID", Types.STRING)
                .field("SOC", Types.DOUBLE)
                .field("HighestVoltage", Types.DOUBLE);

        FormatDescriptor formatDescriptor = new Json().failOnMissingField(false).deriveSchema();
        ste.connect(kafka08).withFormat(formatDescriptor).withSchema(schema).inAppendMode().registerTableSource("SourceTable");
        Table table = ste.sqlQuery("select * from SourceTable").addOrReplaceColumns("AAA(UptTimeMs) AS UptTimeMs");


        table.printSchema();
        //table.addOrReplaceColumns("AAA(UptTimeMs) AS X");


        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);


        rowDataStream.print();

        ste.execute("ABC");
    }

    private static void FunB(StreamTableEnvironment ste) throws Exception {


        TypeInformation<?>[] typeInformations = new TypeInformation<?>[]{Types.SQL_TIMESTAMP, Types.STRING, Types.DOUBLE, Types.DOUBLE};
        String[] fieldNamesA = new String[]{"UptTimeMs", "BillId", "SOC", "HighestVoltage"};
        //TypeInformation<?>[] types = new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.DOUBLE};
        //String[] fieldNamesA = new String[]{"BillId", "SOC", "HighestVoltage"};
        TypeInformation<Row> typeInformation = Types.ROW_NAMED(fieldNamesA, typeInformations);
        TableSchema tableSchemaA = new TableSchema(fieldNamesA, typeInformations);


        DataType[] dataTypes = new DataType[]{DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE()};
        String[] fieldNamesB = new String[]{"UptTimeMs", "BillId", "SOC", "HighestVoltage"};
        TableSchema tableSchemaB = TableSchema.builder().fields(fieldNamesB, dataTypes).build();

        JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema(tableSchemaA.toRowType());

        // 这个builder好奇怪~~
        JsonRowDeserializationSchema jsonRowDeserializationSchemaB = new JsonRowDeserializationSchema.Builder(typeInformation).build();


        //RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("UptTimeMs", new ExistingField("UptTimeMs"), new BoundedOutOfOrderTimestamps(10));

        Kafka08TableSource tableSource = new Kafka08TableSource(
                tableSchemaA,
                Optional.empty(),
                Arrays.asList(),
                Optional.empty(),
                "BDP-ChargingMinuteMetric",
                getKafkaProperties(),
                jsonRowDeserializationSchema,
                StartupMode.EARLIEST,
                Collections.emptyMap());

        ste.registerTableSource("SourceTable", tableSource);
        //Table table = ste.sqlQuery("SELECT FROM_UNIXTIME(UptTimeMs) FROM SourceTable");
        Table table = ste.sqlQuery("SELECT * FROM SourceTable");

        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);
        rowDataStream.print();

        ste.execute("ABC");

    }

    private static void FunA(StreamTableEnvironment ste) throws Exception {
        Kafka kafka08 = new Kafka()
                .version("0.8")
                .topic("BDP-ChargingMinuteMetric")
                .startFromEarliest()
                .property("zookeeper.connect", "hdpjntest.chinacloudapp.cn:2182/kafka08")
                .property("bootstrap.servers", "telddruidteal.chinacloudapp.cn:9095")
                .property("group.id", "abc");


        Schema schema = new Schema()
                .field("UptTimeMs", Types.SQL_TIMESTAMP)
                .rowtime(new Rowtime().timestampsFromField("UptTimeMs").watermarksPeriodicBounded(1000))
                .field("BillID", Types.STRING)
                .field("SOC", Types.DOUBLE)
                .field("HighestVoltage", Types.DOUBLE);


        //.rowtime(new Rowtime().timestampsFromField("UptTimeMs").watermarksPeriodicBounded(1000))


        TypeInformation<?>[] types = new TypeInformation<?>[]{Types.SQL_TIMESTAMP, Types.STRING, Types.DOUBLE, Types.DOUBLE};
        String[] fieldNames = new String[]{"UptTimeMs", "BillId", "SOC", "HighestVoltage"};
        //TypeInformation<?>[] types = new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.DOUBLE};
        //String[] fieldNames = new String[]{"BillId", "SOC", "HighestVoltage"};
        TypeInformation<Row> typeInformation = Types.ROW_NAMED(fieldNames, types);

        //FormatDescriptor formatDescriptor = new Json().failOnMissingField(false).schema(typeInformation).deriveSchema();
        FormatDescriptor formatDescriptor = new Json().failOnMissingField(false).deriveSchema();

        ste.connect(kafka08).withFormat(formatDescriptor).withSchema(schema).inAppendMode().registerTableSource("SourceTable");
        Table table = ste.sqlQuery("select * from SourceTable");

        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);
        rowDataStream.print();

        ste.execute("ABC");


    }

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "hdpjntest.chinacloudapp.cn:2182/kafka08");
        properties.setProperty("bootstrap.servers", "telddruidteal.chinacloudapp.cn:9095");
        properties.setProperty("group.id", "abc");
        return properties;
    }
}
