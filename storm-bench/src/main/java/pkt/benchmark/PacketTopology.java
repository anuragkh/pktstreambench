package pkt.benchmark;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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

import java.util.Map;

public class PacketTopology {
    private static final byte[] pktData = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

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

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("pkt", new PktSpout(), 1);
        builder.setBolt("processed_pkts", new NoOpBolt(), 1).shuffleGrouping("pkt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", new Config(), builder.createTopology());
        org.apache.storm.utils.Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
