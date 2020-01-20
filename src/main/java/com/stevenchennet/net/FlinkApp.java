package com.stevenchennet.net;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.calcite.sql.validate.SqlAbstractConformance;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.Kafka08TableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import scala.Option;

import java.util.*;

public class FlinkApp {
    public static void main(String[] args) throws Exception {
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig).setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment ste = StreamTableEnvironment.create(env);

        FunG(ste);

    }

    private static void FunG(StreamTableEnvironment ste) throws Exception {
        ste.registerFunction("AAA", new LogToRCF339DateTime());
        ste.registerFunction("LongToTs", new LongToTimestamp());

        TypeInformation<?>[] typeInformations = new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.STRING, Types.INT, Types.LONG};
        String[] fieldNamesA = new String[]{"UptTime", "UptTimeStr", "Name", "Hight", "AB"};
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(fieldNamesA, typeInformations);
        TableSchema tableSchemaA = new TableSchema(fieldNamesA, typeInformations);


        // 这个builder好奇怪~~
        JsonRowDeserializationSchema jsonRowDeserializationSchemaB = new JsonRowDeserializationSchema.Builder(rowTypeInformation).build();
        //JsonRowDeserializationSchema jsonRowDeserializationSchemaA = new JsonRowDeserializationSchema(tableSchemaA.toRowType());


        RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("UptTime", new ExistingField("UptTime"), new BoundedOutOfOrderTimestamps(0));
        List<RowtimeAttributeDescriptor> listDesc = Arrays.asList(rowtimeAttributeDescriptor);

        Kafka08TableSource tableSource = new Kafka08TableSource(
                tableSchemaA,
                Optional.empty(),
                Arrays.asList(),
                Optional.empty(),
                "Flink191TestC",
                getKafkaProperties(),
                jsonRowDeserializationSchemaB,
                StartupMode.EARLIEST,
                Collections.emptyMap());

        ste.registerTableSource("SourceTable", tableSource);
        String sqlA = "SELECT * FROM SourceTable";
        Table tableA = ste.sqlQuery(sqlA);
        ste.registerTable("TableA", tableA);
        tableA.printSchema();

        String sqlB = "SELECT UptTime,UptTimeStr,Name,Hight,AB FROM TableA";
        //String sqlB = "SELECT LongToTs(UptTime) AS UptTime,UptTimeStr,Name,Hight,AB FROM TableA";
        Table tableB = ste.sqlQuery(sqlB);
        tableB.printSchema();

        DataStream<Row> rowDataStream = ste.toAppendStream(tableB, Row.class);
        BoundedOutOfOrdernessTimestampExtractor<Row> te = new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0L)) {
            @Override
            public long extractTimestamp(Row row) {
                String teStr = row.getField(0).toString();
                Long ts = Long.parseLong(teStr);
                return ts;
            }
        };
        DataStream<Row> watermarksDataStream = rowDataStream.assignTimestampsAndWatermarks(te);




        watermarksDataStream.print();

        ste.execute("ABC");

    }

    /**
     * 验证 {"UptTime":"2019-03-18T10:01:37Z","Name":"zhangsan","Hight":10,"AB":2} 格式的Json可以运行
     *
     * @param ste
     * @throws Exception
     */
    private static void FunE(StreamTableEnvironment ste) throws Exception {
        ste.registerFunction("AAA", new LogToRCF339DateTime());

        TypeInformation<?>[] typeInformations = new TypeInformation<?>[]{Types.SQL_TIMESTAMP, Types.STRING, Types.INT, Types.LONG};
        String[] fieldNamesA = new String[]{"UptTime", "Name", "Hight", "AB"};
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(fieldNamesA, typeInformations);
        TableSchema tableSchemaA = new TableSchema(fieldNamesA, typeInformations);


        // 这个builder好奇怪~~
        JsonRowDeserializationSchema jsonRowDeserializationSchemaB = new JsonRowDeserializationSchema.Builder(rowTypeInformation).build();
        //JsonRowDeserializationSchema jsonRowDeserializationSchemaA = new JsonRowDeserializationSchema(tableSchemaA.toRowType());


        RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("UptTime", new ExistingField("UptTime"), new BoundedOutOfOrderTimestamps(0));
        List<RowtimeAttributeDescriptor> listDesc = Arrays.asList(rowtimeAttributeDescriptor);

        Kafka08TableSource tableSource = new Kafka08TableSource(
                tableSchemaA,
                Optional.empty(),
                listDesc,
                Optional.empty(),
                "Flink191TestA",
                getKafkaProperties(),
                jsonRowDeserializationSchemaB,
                StartupMode.EARLIEST,
                Collections.emptyMap());

        ste.registerTableSource("SourceTable", tableSource);
        String sqlA = "SELECT * FROM SourceTable";
        //String sqlB = "SELECT TUMBLE_START(UptTime, INTERVAL '10' MINUTE) as STARTXX, Name, MAX(Hight) AS MAXHight FROM SourceTable GROUP BY TUMBLE(UptTime, INTERVAL '10' MINUTE),Name";
        String sqlB = "SELECT TUMBLE_START(UptTime, INTERVAL '10' SECOND) as STARTXX, Name, MAX(Hight) AS MAXHight FROM SourceTable GROUP BY TUMBLE(UptTime, INTERVAL '10' SECOND),Name";
        Table table = ste.sqlQuery(sqlB);
        table.printSchema();


        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);

        rowDataStream.print();

        ste.execute("ABC");

    }

    private static void FunD(StreamTableEnvironment ste) throws Exception {
        ste.registerFunction("AAA", new LogToRCF339DateTime());

        TypeInformation<?>[] typeInformations = new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE};
        String[] fieldNamesA = new String[]{"UptTimeMs", "BillID", "SOC", "HighestVoltage"};
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(fieldNamesA, typeInformations);
        TableSchema tableSchemaA = new TableSchema(fieldNamesA, typeInformations);


        DataType[] dataTypes = new DataType[]{DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE()};
        String[] fieldNamesB = new String[]{"UptTimeMs", "BillID", "SOC", "HighestVoltage"};
        TableSchema tableSchemaB = TableSchema.builder().fields(fieldNamesB, dataTypes).build();


        // 这个builder好奇怪~~
        JsonRowDeserializationSchema jsonRowDeserializationSchemaB = new JsonRowDeserializationSchema.Builder(rowTypeInformation).build();
        //JsonRowDeserializationSchema jsonRowDeserializationSchemaA = new JsonRowDeserializationSchema(tableSchemaA.toRowType());


        RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("UptTimeMs", new ExistingField("UptTimeMs"), new BoundedOutOfOrderTimestamps(10));
        List<RowtimeAttributeDescriptor> listDesc = Arrays.asList(rowtimeAttributeDescriptor);

        Kafka08TableSource tableSource = new Kafka08TableSource(
                tableSchemaA,
                Optional.empty(),
                Arrays.asList(),
                Optional.empty(),
                "BDP-ChargingMinuteMetric",
                getKafkaProperties(),
                jsonRowDeserializationSchemaB,
                StartupMode.EARLIEST,
                Collections.emptyMap());

        ste.registerTableSource("SourceTable", tableSource);
        String sqlA = "SELECT * FROM SourceTable";
        Table table = ste.sqlQuery(sqlA).addOrReplaceColumns("AAA(UptTimeMs) AS UptTimeMs");
        table.printSchema();
        Table targetTable = KafkaProtobufSchemaSource.assginTimestampToSchemaTable(table, Option.apply("UptTimeMs"), Option.apply((Object) 10L), ste);
        targetTable.printSchema();
        ste.registerTable("TargetTable", targetTable);

        String sqlB = "SELECT * FROM TargetTable";
        //String sqlB = "SELECT TUMBLE_START(UptTimeMs, INTERVAL '1' SECOND) as STARTXX, BillID, MAX(SOC) AS MaxSOC FROM TargetTable GROUP BY TUMBLE(UptTimeMs, INTERVAL '1' SECOND),BillID";

        Table myTable = ste.sqlQuery(sqlB);
        System.out.println("myTalbe...");
        myTable.printSchema();


        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);

        rowDataStream.print();

        ste.execute("ABC");

    }

    private static void FunC(StreamTableEnvironment ste) throws Exception {
        ste.registerFunction("AAA", new LogToRCF339DateTime());

        TypeInformation<?>[] typeInformations = new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE};
        String[] fieldNamesA = new String[]{"UptTimeMs", "BillID", "SOC", "HighestVoltage"};
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(fieldNamesA, typeInformations);
        TableSchema tableSchemaA = new TableSchema(fieldNamesA, typeInformations);


        DataType[] dataTypes = new DataType[]{DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE()};
        String[] fieldNamesB = new String[]{"UptTimeMs", "BillID", "SOC", "HighestVoltage"};
        TableSchema tableSchemaB = TableSchema.builder().fields(fieldNamesB, dataTypes).build();


        // 这个builder好奇怪~~
        JsonRowDeserializationSchema jsonRowDeserializationSchemaB = new JsonRowDeserializationSchema.Builder(rowTypeInformation).build();
        //JsonRowDeserializationSchema jsonRowDeserializationSchemaA = new JsonRowDeserializationSchema(tableSchemaA.toRowType());


        RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("UptTimeMs", new ExistingField("UptTimeMs"), new BoundedOutOfOrderTimestamps(10));
        List<RowtimeAttributeDescriptor> listDesc = Arrays.asList(rowtimeAttributeDescriptor);

        Kafka08TableSource tableSource = new Kafka08TableSource(
                tableSchemaA,
                Optional.empty(),
                Arrays.asList(),
                Optional.empty(),
                "BDP-ChargingMinuteMetric",
                getKafkaProperties(),
                jsonRowDeserializationSchemaB,
                StartupMode.EARLIEST,
                Collections.emptyMap());

        ste.registerTableSource("SourceTable", tableSource);
        String sqlA = "SELECT * FROM SourceTable";
        Table table = ste.sqlQuery(sqlA).addOrReplaceColumns("AAA(UptTimeMs) AS UptTimeMs");
        table.printSchema();
        Table targetTable = KafkaProtobufSchemaSource.assginTimestampToSchemaTable(table, Option.apply("UptTimeMs"), Option.apply((Object) 10L), ste);
        targetTable.printSchema();
        ste.registerTable("TargetTable", targetTable);

        String sqlB = "SELECT TUMBLE_END(UptTimeMs, INTERVAL '1' SECOND) as START, BillId, MAX(SOC) AS MaxSOC FROM TargetTable GROUP BY TUMBLE(UptTimeMs, INTERVAL '1' SECOND),BillId";


        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);
        rowDataStream.print();

        ste.execute("ABC");

    }

    private static void FunB(StreamTableEnvironment ste) throws Exception {
        TypeInformation<?>[] typeInformations = new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE};
        String[] fieldNamesA = new String[]{"UptTimeMs", "BillID", "SOC", "HighestVoltage"};
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(fieldNamesA, typeInformations);
        TableSchema tableSchemaA = new TableSchema(fieldNamesA, typeInformations);


        DataType[] dataTypes = new DataType[]{DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE()};
        String[] fieldNamesB = new String[]{"UptTimeMs", "BillID", "SOC", "HighestVoltage"};
        TableSchema tableSchemaB = TableSchema.builder().fields(fieldNamesB, dataTypes).build();


        // 这个builder好奇怪~~
        JsonRowDeserializationSchema jsonRowDeserializationSchemaB = new JsonRowDeserializationSchema.Builder(rowTypeInformation).build();
        //JsonRowDeserializationSchema jsonRowDeserializationSchemaA = new JsonRowDeserializationSchema(tableSchemaA.toRowType());


        RowtimeAttributeDescriptor rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor("UptTimeMs", new ExistingField("UptTimeMs"), new BoundedOutOfOrderTimestamps(10));
        List<RowtimeAttributeDescriptor> listDesc = Arrays.asList(rowtimeAttributeDescriptor);

        Kafka08TableSource tableSource = new Kafka08TableSource(
                tableSchemaA,
                Optional.empty(),
                Arrays.asList(),
                Optional.empty(),
                "BDP-ChargingMinuteMetric",
                getKafkaProperties(),
                jsonRowDeserializationSchemaB,
                StartupMode.EARLIEST,
                Collections.emptyMap());

        ste.registerTableSource("SourceTable", tableSource);
        //Table table = ste.sqlQuery("SELECT FROM_UNIXTIME(UptTimeMs) FROM SourceTable");
        String sqlA = "SELECT * FROM SourceTable";
        Table table = ste.sqlQuery(sqlA);
        table.printSchema();


        DataStream<Row> rowDataStream = ste.toAppendStream(table, Row.class);
        rowDataStream.print();

        ste.execute("ABC");

    }

    private static void FunA(StreamTableEnvironment ste) throws Exception {
        Kafka kafka08 = new Kafka()
                .version("0.8")
                .topic("BDP-ChargingMinuteMetric")
                .startFromEarliest()
                .properties(getKafkaProperties());


        Schema schema = new Schema()
                .field("UptTimeMs", Types.LONG)
                //.rowtime(new Rowtime().timestampsFromField("UptTimeMs").watermarksPeriodicBounded(1000))
                .field("BillID", Types.STRING)
                .field("SOC", Types.DOUBLE)
                .field("HighestVoltage", Types.DOUBLE);


        //.rowtime(new Rowtime().timestampsFromField("UptTimeMs").watermarksPeriodicBounded(1000))


        TypeInformation<?>[] types = new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE};
        String[] fieldNames = new String[]{"UptTimeMs", "BillId", "SOC", "HighestVoltage"};
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
        properties.setProperty("group.id", "abc2");
        return properties;
    }
}
