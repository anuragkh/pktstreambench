package pkt.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import pkt.benchmark.generator.PktGenerator;
import pkt.benchmark.utils.ThroughputLogger;

import java.util.Properties;

public class ThroughputBenchmark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        DataStream<byte[]> pkts = env.addSource(new PktGenerator());
        pkts.map(new ThroughputLogger(54, 1_000_000))
                .addSink(new FlinkKafkaProducer08<>("pkt", (SerializationSchema<byte[]>) bytes -> bytes, props));
        env.execute("Packet Throughput Benchmark");
    }
}
