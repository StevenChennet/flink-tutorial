package com.stevenchennet.net.cogroup;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class AppCoGroup {
    public static void main(String[] args) throws Exception{
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig).setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Bms> bmsSource = env.addSource(new BmsSource());
        DataStreamSource<KwhMeter> kwhMeterSource = env.addSource(new KwhMeterSource());
        DataStreamSource<PileData> pileDataSource = env.addSource(new PileDataSource());

        bmsSource.coGroup(kwhMeterSource)
                .where(bms->bms.getBillId()).equalTo(kwhMeter->kwhMeter.getBillId())
                .window(TumblingEventTimeWindows.of(Time.seconds(60L)))
                .apply(new MyCoGroupFunction());
    }

    static class MyCoGroupFunction implements CoGroupFunction<Bms, KwhMeter, MinuteMetric>{
        @Override
        public void coGroup(Iterable<Bms> first, Iterable<KwhMeter> second, Collector<MinuteMetric> out) throws Exception {

        }
    }
}
