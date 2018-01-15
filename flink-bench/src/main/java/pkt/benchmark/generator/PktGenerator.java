package pkt.benchmark.generator;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class PktGenerator extends RichParallelSourceFunction<byte[]> {
    private boolean running = true;
    private static final byte[] pktData = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    @Override
    public void run(SourceContext<byte[]> context) throws Exception {
        while (running) {
            context.collect(pktData);
        }
        context.close();
    }

    @Override
    public void cancel() {
        running = false;
    }

}
