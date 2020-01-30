package com.stevenchennet.net.cogroup;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public class BmsSource implements SourceFunction<Bms> {
    @Override
    public void run(SourceContext<Bms> ctx) throws Exception {
        long TAG = 1000L;
        List<Bms> bmsList = Arrays.asList(
                Bms.builder().id("A").billId("billId").uptTime(110 * TAG).soc(0.1).tMax(100D).tMaxCode("A").build(),
                Bms.builder().id("A").billId("billId").uptTime(140 * TAG).soc(0.2).tMax(110D).tMaxCode("A").build(),
                Bms.builder().id("A").billId("billId").uptTime(170 * TAG).soc(0.3).tMax(120D).tMaxCode("A").build(),
                Bms.builder().id("A").billId("billId").uptTime(200 * TAG).soc(0.4).tMax(130D).tMaxCode("B").build(),
                Bms.builder().id("A").billId("billId").uptTime(230 * TAG).soc(0.5).tMax(100D).tMaxCode("B").build(),
                Bms.builder().id("A").billId("billId").uptTime(260 * TAG).soc(0.6).tMax(100D).tMaxCode("B").build(),
                Bms.builder().id("A").billId("billId").uptTime(290 * TAG).soc(0.7).tMax(100D).tMaxCode("C").build(),
                Bms.builder().id("A").billId("billId").uptTime(320 * TAG).soc(0.8).tMax(100D).tMaxCode("D").build(),
                Bms.builder().id("A").billId("billId").uptTime(350 * TAG).soc(0.9).tMax(100D).tMaxCode("D").build(),
                Bms.builder().id("A").billId("billId").uptTime(380 * TAG).soc(1.0).tMax(100D).tMaxCode("D").build()
        );

        for (Bms bms : bmsList){
            ctx.collectWithTimestamp(bms,bms.getUptTime());
        }
    }

    @Override
    public void cancel() {

    }
}
