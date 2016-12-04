package storm.benchmark;

import benchmark.common.CommonConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * This is a basic example of a Storm topology.
 */
public class StormBenchmark {

    public static class DeserializeBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {

            JSONObject obj = new JSONObject(tuple.getString(0));
            String geo = obj.getString("key");
            Double price = obj.getDouble("value");
            Long ts = obj.getLong("ts");
            _collector.emit(tuple, new Values(
                    geo,
                    ts,
                    price
          	  ));
             _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("geo", "ts", "price"));
        }
    }

    public static class DeserializeAndGroupBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {

            JSONObject obj = new JSONObject(tuple.getString(0));
            String group = obj.getString("key") + obj.getDouble("value");
            Long ts = obj.getLong("ts");
            _collector.emit(tuple, new Values(
                    group,
                    ts
            ));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("group", "ts"));
        }
    }


    public static class FilterBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("geo", "ts", "price"));
        }
    }


    public static class SlidingWindowAvgBolt extends BaseWindowedBolt {

        private HashMap<String,Double> sumState = new HashMap<>();
        private OutputCollector collector;
        private HashMap<String,Integer> sizeState = new HashMap<>();
        private HashMap<String,Long> tsMap = new HashMap<>();

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            /*
             * The inputWindow gives a view of
             * (a) all the events in the window
             * (b) events that expired since last activation of the window
             * (c) events that newly arrived since last activation of the window
             */
            List<Tuple> newTuples = inputWindow.getNew();
            List<Tuple> expiredTuples = inputWindow.getExpired();

            /*
             * Instead of iterating over all the tuples in the window to compute
             * the sum, the values for the new events are added and old events are
             * subtracted. Similar optimizations might be possible in other
             * windowing computations.
             */
            for (Tuple tuple : newTuples) {
                String key = tuple.getString(0);
                sumState.put(key, sumState.getOrDefault(key,0.0 )  +  tuple.getDouble(2)    ) ;
                tsMap.put(key, Math.max(tsMap.getOrDefault(key,0L), tuple.getLong(1)));
                sizeState.put(key, sizeState.getOrDefault(key, 0  )  +  1    ) ;
            }
            for (Tuple tuple : expiredTuples) {
                String key = tuple.getString(0);
                sumState.put(key, sumState.get(key )  -  tuple.getDouble(2)    ) ;
                sizeState.put(key, sizeState.getOrDefault(key, 0  )  -  1    ) ;
            }

            for (Map.Entry<String, Double> entry : sumState.entrySet()) {
                String geo = entry.getKey();
                Double sum = entry.getValue();
                collector.emit(new Values(geo, tsMap.get(geo), sum / sizeState.get(geo), sizeState.get(geo)));
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("geo","ts","avg_price", "window_size"));
        }

    }

    public static class FinalTSJoinBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values( System.currentTimeMillis() - tuple.getLongByField("max_ts"), tuple.getLongByField("max_ts") ));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields( "interval", "max_ts"));
        }
    }



    public static class FinalTSBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Long ts = System.currentTimeMillis() -  tuple.getLong(1);
            _collector.emit(tuple, new Values(tuple.getString(0), ts , tuple.getLong(1), tuple.getDouble(2), tuple.getInteger(3) ));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields( "geo","ts","ts_start","avg_price", "window_size"));
        }
    }



    private static HdfsBolt createSink() {
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(",");

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(CommonConfig.OUTPUT_SYNC_POLICY_COUNT());

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(CommonConfig.OUTPUT_BATCHSIZE_KB(), FileSizeRotationPolicy.Units.KB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(CommonConfig.STORM_OUTPUT());

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(CommonConfig.HDFS_URI())
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        return bolt;

    }


    private static StormTopology windowedAggregation(TopologyBuilder builder){
        for (String host: CommonConfig.DATASOURCE_HOSTS()){
            for(Integer port: CommonConfig.DATASOURCE_PORTS()){
                builder.setSpout("source"+host + "" + port, new SocketReceiver(host, port),1);
            }
        }
        BoltDeclarer bolt= builder.setBolt("event_deserializer", new DeserializeBolt(), CommonConfig.PARALLELISM());
        for (String host: CommonConfig.DATASOURCE_HOSTS()){
            for(Integer port: CommonConfig.DATASOURCE_PORTS()) {
                bolt = bolt.shuffleGrouping("source"+host +"" + port);
            }
        }
        builder.setBolt("sliding_avg", new SlidingWindowAvgBolt()
                .withWindow(new Duration(CommonConfig.SLIDING_WINDOW_LENGTH(), TimeUnit.MILLISECONDS),
                        new Duration(CommonConfig.SLIDING_WINDOW_SLIDE(), TimeUnit.MILLISECONDS))
                ,CommonConfig.PARALLELISM()).fieldsGrouping("event_deserializer", new Fields("geo") );
        builder.setBolt("event_filter", new FinalTSBolt(), CommonConfig.PARALLELISM()).shuffleGrouping("sliding_avg");
        builder.setBolt("hdfsbolt", createSink(), CommonConfig.PARALLELISM()).shuffleGrouping("event_filter");
        return builder.createTopology();

    }

    private static StormTopology dummyConsumer(TopologyBuilder builder) {
        for (String host: CommonConfig.DATASOURCE_HOSTS()){
            for(Integer port: CommonConfig.DATASOURCE_PORTS()){
                builder.setSpout("source"+host + "" + port, new SocketReceiver(host, port),CommonConfig.PARALLELISM());
            }
        }
        BoltDeclarer bolt= builder.setBolt("event_filter", new FilterBolt(), CommonConfig.PARALLELISM());
        for (String host: CommonConfig.DATASOURCE_HOSTS()){
            for(Integer port: CommonConfig.DATASOURCE_PORTS()) {
                bolt = bolt.shuffleGrouping("source"+host + "" + port);
            }
        }
        builder.setBolt("hdfsbolt", createSink(), CommonConfig.PARALLELISM()).shuffleGrouping("event_filter");
        return builder.createTopology();
    }


    public static class SlidingWindowJoinBolt extends BaseWindowedBolt {

        private OutputCollector collector;
        private HashMap<String, Set<Long>> probeMap = new HashMap<>();
        private String probeStreamID;
        public SlidingWindowJoinBolt(String probeStream){
            this.probeStreamID = probeStream;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {

            List<Tuple> newTestTuples = new ArrayList<>();

            for (Tuple t : inputWindow.getExpired() ){
                if (t.getSourceGlobalStreamId().get_componentId().equals(probeStreamID)){
                    String key = t.getStringByField("group");
                    Set s = probeMap.get(key);
                    s.remove(t.getLongByField("ts"));
                    if (s.size() == 0){
                        probeMap.remove(key);
                    } else {
                        probeMap.put(key, s);
                    }
                }
            }
            for (Tuple t: inputWindow.getNew()){
                if (t.getSourceGlobalStreamId().get_componentId().equals(probeStreamID)){
                    String key = t.getStringByField("group");
                    Set s = probeMap.getOrDefault(key, new HashSet<>());
                    s.add(t.getLongByField("ts"));
                    probeMap.put(key,s);
                } else {
                    newTestTuples.add(t);
                }
            }

            for (Tuple t: newTestTuples){
                String key = t.getStringByField("group");
                if (probeMap.containsKey(key)){
                    Set<Long> s = probeMap.get(key);
                    for (Long ts : s){
                        collector.emit(new Values(  Math.max( t.getLongByField("ts"), ts))   );
                    }
                }
            }



        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("max_ts"));
        }

    }



    private static StormTopology windowedJoin(TopologyBuilder builder){
        for (String host: CommonConfig.DATASOURCE_HOSTS()){
            for(Integer port: CommonConfig.DATASOURCE_PORTS()) {
                builder.setSpout("source"+host + "" + port, new SocketReceiver(host, port),1);
            }
        }
        BoltDeclarer joinBolt1= builder.setBolt("event_deserializer1", new DeserializeAndGroupBolt(), CommonConfig.PARALLELISM());
        BoltDeclarer joinBolt2= builder.setBolt("event_deserializer2", new DeserializeAndGroupBolt(), CommonConfig.PARALLELISM());

        int i = 0;
        for (String host: CommonConfig.DATASOURCE_HOSTS()) {
            for (Integer port : CommonConfig.DATASOURCE_PORTS()) {
                if (i % 2 == 0){
                    joinBolt1 = joinBolt1.shuffleGrouping("source"+host + "" + port);
                } else {
                    joinBolt2 = joinBolt2.shuffleGrouping("source"+host + "" + port);
                }
                i++;
            }
        }

        builder.setBolt("sliding_join", new SlidingWindowJoinBolt("event_deserializer1")
                .withWindow(new Duration(CommonConfig.SLIDING_WINDOW_LENGTH(), TimeUnit.MILLISECONDS),
                        new Duration(CommonConfig.SLIDING_WINDOW_SLIDE(), TimeUnit.MILLISECONDS)),CommonConfig.PARALLELISM())
                .fieldsGrouping("event_deserializer1", new Fields("group") )
                .fieldsGrouping("event_deserializer2", new Fields("group") );
        builder.setBolt("event_filter", new FinalTSJoinBolt(), CommonConfig.PARALLELISM()).shuffleGrouping("sliding_join");
        builder.setBolt("hdfsbolt", createSink(), CommonConfig.PARALLELISM()).shuffleGrouping("event_filter");
        return builder.createTopology();

    }


    public static void main(String[] args) throws Exception {

        String confPath = args[0];
        String runningMode = args[1];
        TopologyBuilder builder = new TopologyBuilder();

        CommonConfig.initializeConfig(confPath);
        StormTopology topology = null;
        if(CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.AGGREGATION_USECASE)){
            topology = windowedAggregation(builder);
        } else if(CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.JOIN_USECASE)){
            topology = windowedJoin(builder);
        } else if (CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.DUMMY_CONSUMER)){
            topology = dummyConsumer(builder);
        }

        Config conf = new Config();
        if (runningMode.equals("cluster")) {
//            conf.setNumWorkers(CommonConfig.STORM_WORKERS());
  //          conf.setNumAckers(CommonConfig.STORM_ACKERS());
            StormSubmitter.submitTopologyWithProgressBar(args[2], conf, topology);
        } else if (runningMode.equals("local")) {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(args[2], conf, topology);
//            backtype.storm.utils.Utils.sleep(10000);
//            cluster.killTopology("test");
//            cluster.shutdown();
        } else {
            throw new Exception("Second commandline argument should be local or cluster");
        }
    }

}
