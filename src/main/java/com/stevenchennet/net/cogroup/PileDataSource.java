package com.stevenchennet.net.cogroup;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public class PileDataSource implements SourceFunction<PileData> {
    @Override
    public void run(SourceContext<PileData> ctx) throws Exception {
        long TAG = 1000;
        List<PileData> pileDataList = Arrays.asList(
                PileData.builder().id("A").billId("billId").uptTime(110 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(140 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(170 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(200 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(230 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(260 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(290 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(320 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(350 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build(),
                PileData.builder().id("A").billId("billId").uptTime(380 * TAG).DirectCurrent(30).DirectVoltage(380).DirectPower(10000).build()
        );

        for (PileData pileData : pileDataList){
            ctx.collectWithTimestamp(pileData,pileData.getUptTime());
        }
    }

    @Override
    public void cancel() {

    }
}
