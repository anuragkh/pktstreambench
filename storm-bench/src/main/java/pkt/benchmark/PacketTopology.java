package pkt.benchmark;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;

public class PacketTopology {
    static Logger LOG = LoggerFactory.getLogger(PacketTopology.class);

    private static final byte[] pktData = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    public static class PktSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            _collector.emit(new Values(pktData));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("pkt"));
        }
    }

    public static class NoOpBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, tuple.getValues());
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("pkt"));
        }
    }

    public static class ThroughputBolt extends BaseRichBolt {
        long received = 0;
        long start = 0;
        private OutputCollector _collector;
        private long logfreq;
        private long lastLog = -1;
        private long lastPkts;

        ThroughputBolt(long logfreq) {
            this.logfreq = logfreq;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            if (start == 0) {
                start = System.currentTimeMillis();
            }
            received++;
            if (received % logfreq == 0) {
                long now = System.currentTimeMillis();

                // throughput for the last "logfreq" elements
                if (lastLog == -1) {
                    // init (the first)
                    lastLog = now;
                    lastPkts = received;
                } else {
                    long timeDiff = now - lastLog;
                    long pktDiff = received - lastPkts;
                    double ex = (1000 / (double) timeDiff);
                    LOG.info("During the last {} ms, we received {} pkts. That's {} pkts/second/core", timeDiff,
                            pktDiff, pktDiff * ex);
                    // reinit
                    lastLog = now;
                    lastPkts = received;
                }
            }

            _collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("pkt"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaBolt<String, byte[]> sink = new KafkaBolt<String, byte[]>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("test"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "pkt"));
        builder.setSpout("pkt", new PktSpout(), 1);
        // builder.setBolt("processed_pkt", new NoOpBolt(), 1).shuffleGrouping("pkt");
        // builder.setBolt("thput_pkt", new ThroughputBolt(1_000_000), 1).shuffleGrouping("pkt");
        builder.setBolt("fwdToKafka", sink, 1).shuffleGrouping("pkt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", new Config(), builder.createTopology());
        org.apache.storm.utils.Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
