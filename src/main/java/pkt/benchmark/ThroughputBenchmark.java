package pkt.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pkt.benchmark.generator.PktGenerator;
import pkt.benchmark.utils.ThroughputLogger;

public class ThroughputBenchmark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<byte[]> pkts = env.addSource(new PktGenerator());
        pkts.flatMap(new ThroughputLogger(54, 1_000_000));
    }
}
