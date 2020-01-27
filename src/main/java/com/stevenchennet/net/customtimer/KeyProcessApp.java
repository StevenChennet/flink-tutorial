package com.stevenchennet.net.customtimer;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class KeyProcessApp {
    public static void main(String[] args) {
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig).setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Bms> bmsDataStreamSource = env.addSource(new BmsSource());

        SingleOutputStreamOperator<Bms> bmsSourceStream = bmsDataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Bms>(Time.seconds(0L)) {
            @Override
            public long extractTimestamp(Bms bms) {
                return bms.getUptTime();
            }
        });

        KeyedStream<Bms, String> keyedBmsStream = bmsSourceStream.keyBy(bms -> bms.getId());
        //WindowAssigner<Object, TimeWindow> bmsWindowAssigner = TumblingEventTimeWindows.of(Time.seconds(10L), Time.seconds(0L));

        WindowedStream<Bms,String, TimeWindow> timeWindowedBmsStream =  keyedBmsStream.window(TumblingEventTimeWindows.of(Time.seconds(10L), Time.seconds(0L)));

        WindowedStream<Bms,String, TimeWindow> triggerTimeWindowedBmsStream = timeWindowedBmsStream.trigger(new Trigger<Bms, TimeWindow>() {
            @Override
            public TriggerResult onElement(Bms bms, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

            }
        });

        SingleOutputStreamOperator<Bms> bmsStream =  timeWindowedBmsStream.process(new ProcessWindowFunction<Bms, Bms, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Bms> iterable, Collector<Bms> collector) throws Exception {

            }
        });





    }


}
