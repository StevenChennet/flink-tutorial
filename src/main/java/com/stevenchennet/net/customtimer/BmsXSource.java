package com.stevenchennet.net.customtimer;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public class BmsXSource implements SourceFunction<Bms> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Bms> sourceContext) throws Exception {
        long TAG = 1000L;
        List<Bms> bmsList = Arrays.asList(
                Bms.builder().id("A").uptTime(110 * TAG).soc(0.1).tMax(100D).tMaxCode("A").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(140 * TAG).soc(0.2).tMax(110D).tMaxCode("A").kwhMeter(123D).build(),
                //Bms.builder().id("A").uptTime(170 * TAG).soc(0.3).tMax(120D).tMaxCode("A").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(200 * TAG).soc(0.4).tMax(130D).tMaxCode("B").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(230 * TAG).soc(0.5).tMax(100D).tMaxCode("B").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(260 * TAG).soc(0.6).tMax(100D).tMaxCode("B").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(290 * TAG).soc(0.7).tMax(100D).tMaxCode("C").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(170 * TAG).soc(0.3).tMax(120D).tMaxCode("A").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(320 * TAG).soc(0.8).tMax(100D).tMaxCode("D").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(350 * TAG).soc(0.9).tMax(100D).tMaxCode("D").kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(380 * TAG).soc(1.0).tMax(100D).tMaxCode("D").kwhMeter(123D).build()
        );

        //bmsList.forEach(sourceContext::collect);

        for (Bms bms : bmsList){
            sourceContext.collect(bms);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

