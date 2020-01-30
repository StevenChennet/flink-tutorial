package com.stevenchennet.net.cogroup;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class PileDataSource implements SourceFunction<PileData> {
    @Override
    public void run(SourceContext<PileData> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
