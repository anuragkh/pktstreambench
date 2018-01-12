package pkt.benchmark.generator;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class PktGenerator extends RichParallelSourceFunction<byte[]> {
    private boolean running = true;
    private static final byte[] pktData = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    @Override
    public void run(SourceContext<byte[]> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(pktData);
        }
        sourceContext.close();
    }

    @Override
    public void cancel() {
        running = false;
    }

}
