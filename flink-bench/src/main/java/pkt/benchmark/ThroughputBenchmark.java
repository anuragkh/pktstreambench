package pkt.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import pkt.benchmark.generator.PktGenerator;

import java.util.Properties;

public class ThroughputBenchmark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<byte[]> pkts = env.addSource(new PktGenerator());
        // pkts.flatMap(new ThroughputLogger(54, 1_000_000));
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        props.setProperty("bootstrap.servers", "localhost:9092");
        pkts.addSink(new FlinkKafkaProducer08<>("pkt", (SerializationSchema<byte[]>) bytes -> bytes, props));
        env.execute("Packet Throughput Benchmark");
    }
}
