package com.stevenchennet.net.customtimer;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BmsSource implements SourceFunction<Bms>, CheckpointedFunction {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Bms> sourceContext) throws Exception {
        List<Bms> bmsList = Arrays.asList(
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build(),
                Bms.builder().id("A").uptTime(1).soc(0.1).tMax(100D).tMin(15D).kwhMeter(123D).build()
        );
        bmsList.forEach(sourceContext::collect);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    //region checkpoint Function
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
    // endregion
}
