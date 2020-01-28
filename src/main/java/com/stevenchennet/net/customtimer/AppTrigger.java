package com.stevenchennet.net.customtimer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AppTrigger {
    public static void main(String[] args) throws Exception {
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

        //WindowedStream<Bms, String, TimeWindow> timeWindowedBmsStream = keyedBmsStream.window(TumblingEventTimeWindows.of(Time.seconds(60L), Time.seconds(0L)));
        WindowedStream<Bms, String, TimeWindow> timeWindowedBmsStream = keyedBmsStream.window(TumblingEventTimeWindows.of(Time.seconds(60L)));

        //WindowedStream<Bms, String, TimeWindow> triggerTimeWindowedBmsStream = timeWindowedBmsStream.trigger(new MyTrigger());
        WindowedStream<Bms, String, TimeWindow> triggerTimeWindowedBmsStream = timeWindowedBmsStream.trigger(new MyTriggerX());

        SingleOutputStreamOperator<MinuteMetric> bmsStream = timeWindowedBmsStream.process(new ProcessWindowFunction<Bms, MinuteMetric, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Bms> iterable, Collector<MinuteMetric> collector) throws Exception {
                List<Bms> bmsList = StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
                MinuteMetric minuteMetric = new MinuteMetric();
                minuteMetric.setId(s);

                TimeWindow window = context.window();
                long windowStart = window.getStart();
                long windowEnd = window.getEnd();
                long maxTimestamp = window.maxTimestamp();
                long waterMark = context.currentWatermark();

                minuteMetric.setUptTimeStart(windowStart);
                minuteMetric.setUptTimeEnd(windowEnd);
                double maxSoc = bmsList.stream().map(Bms::getSoc).max(Double::compareTo).get();
                minuteMetric.setMaxSoc(maxSoc);


                collector.collect(minuteMetric);
            }
        });

        bmsStream.print();

        env.execute();
    }

    static class MyTriggerX extends Trigger<Bms,TimeWindow>{

        @Override
        public TriggerResult onElement(Bms element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            long currentWaterMark = ctx.getCurrentWatermark();
            long start = window.getStart();
            long end = window.getEnd();
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            long currentWaterMark = ctx.getCurrentWatermark();
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            long currentWaterMark = ctx.getCurrentWatermark();
            long start = window.getStart();
            long end = window.getEnd();
            if(time == window.getEnd()){
                return TriggerResult.FIRE_AND_PURGE;
            }else{
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    static class MyTrigger extends Trigger<Bms, TimeWindow> {

        @Override
        public TriggerResult onElement(Bms element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            long currentWaterMark = ctx.getCurrentWatermark();
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Boolean.class));
            if (!firstSeen.value()) {
                long t = currentWaterMark + (1000 - (currentWaterMark % 1000));
                //ctx.registerEventTimeTimer(t);
                //ctx.registerEventTimeTimer(window.getEnd());
                firstSeen.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (time == window.getEnd()) {
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                if(t<window.getEnd()){
                    ctx.registerEventTimeTimer(t);
                }
                return TriggerResult.FIRE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Boolean.class));
            firstSeen.clear();
        }
    }
}
