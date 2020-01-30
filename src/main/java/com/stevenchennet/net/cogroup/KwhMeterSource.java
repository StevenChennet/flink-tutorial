package com.stevenchennet.net.cogroup;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public class KwhMeterSource implements SourceFunction<KwhMeter> {

    @Override
    public void run(SourceContext<KwhMeter> ctx) throws Exception {
        long TAG = 1000L;

        List<KwhMeter> kwhMeterList = Arrays.asList(
                KwhMeter.builder().id("A").billId("billId").uptTime(110 * TAG).kwhMeter(101).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(140 * TAG).kwhMeter(102).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(170 * TAG).kwhMeter(103).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(200 * TAG).kwhMeter(104).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(230 * TAG).kwhMeter(105).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(260 * TAG).kwhMeter(106).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(290 * TAG).kwhMeter(107).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(320 * TAG).kwhMeter(108).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(350 * TAG).kwhMeter(109).build(),
                KwhMeter.builder().id("A").billId("billId").uptTime(380 * TAG).kwhMeter(110).build()
        );

        for (KwhMeter kwhMeter : kwhMeterList){
            ctx.collectWithTimestamp(kwhMeter,kwhMeter.getUptTime());
        }
    }

    @Override
    public void cancel() {

    }
}
