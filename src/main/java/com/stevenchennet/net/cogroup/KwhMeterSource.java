package com.stevenchennet.net.cogroup;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class KwhMeterSource implements SourceFunction<KwhMeter> {

    @Override
    public void run(SourceContext<KwhMeter> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
