package pkt.benchmark.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputLogger implements MapFunction<byte[], byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private int pktSize;
    private long logfreq;

    public ThroughputLogger(int pktSize, long logfreq) {
        this.pktSize = pktSize;
        this.logfreq = logfreq;
    }

    @Override
    public byte[] map(byte[] pkt) throws Exception {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();

            // throughput for the last "logfreq" pkts
            if(lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long pktDiff = totalReceived - lastTotalReceived;
                double ex = (1000/(double)timeDiff);
                LOG.info("During the last {} ms, we processed {} pkts. That's {} pkts/second/core. {} MB/sec/core. GB received {}",
                        timeDiff, pktDiff, pktDiff*ex, pktDiff*ex* pktSize / 1024 / 1024, (totalReceived * pktSize) / 1024 / 1024 / 1024);
                // reinit
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
        return pkt;
    }
}
