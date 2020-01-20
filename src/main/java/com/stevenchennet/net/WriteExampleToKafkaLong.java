package com.stevenchennet.net;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;

public class WriteExampleToKafkaLong {
    public static void main(String[] args) throws Exception {
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig).setParallelism(1);
        StreamTableEnvironment ste = StreamTableEnvironment.create(env);

        DataStreamSource<MyExample> source = env.fromElements(
                // 2019-03-18T10:01:37Z
                new MyExample(1552874497000L, "2019-03-18T10:01:37Z", "zhangsan", 10, 2L),
                // 2019-03-18T10:02:37Z
                new MyExample(1552874557000L, "2019-03-18T10:02:37Z", "zhangsan", 10, 2L),
                // 2019-03-18T10:11:37Z
                new MyExample(1552875097000L, "2019-03-18T10:11:37Z", "zhangsan", 10, 2L),
                // 2019-03-18T10:21:37Z
                new MyExample(1552875697000L, "2019-03-18T10:21:37Z", "zhangsan", 10, 2L),
                // 2019-03-18T10:32:37Z
                new MyExample(1552876357000L, "2019-03-18T10:32:37Z", "zhangsan", 10, 2L)
        );

        FlinkKafkaProducer08<String> sink = new FlinkKafkaProducer08<>("Flink191TestC", new SimpleStringSchema(), getKafkaProperties());

        source.map(t -> t.toJson()).addSink(sink);

        env.execute("ABC");
    }

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "hdpjntest.chinacloudapp.cn:2182/kafka08");
        properties.setProperty("bootstrap.servers", "telddruidteal.chinacloudapp.cn:9095");
        properties.setProperty("group.id", "abcd");
        return properties;
    }

    static class MyExample {
        public MyExample(Long uptTime, String uptTimeStr, String name, int hight, long ab) {
            this.UptTime = uptTime;
            this.UptTimeStr = uptTimeStr;
            this.Name = name;
            this.Hight = hight;
            this.AB = ab;
        }

        public MyExample() {

        }

        public Long UptTime;
        public String UptTimeStr;
        public String Name;
        public int Hight;
        public long AB;

        public String toJson() throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(this);
            return json;
        }
    }
}
