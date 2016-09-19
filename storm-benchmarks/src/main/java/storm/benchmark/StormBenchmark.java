///**
// * Copyright 2015, Yahoo Inc.
// * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
// */
//package storm.benchmark;
//
//import java.io.*;
//import java.net.InetAddress;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.Map;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
//import com.esotericsoftware.yamlbeans.YamlReader;
//import data.source.socket.DataGenerator;
//import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
//import org.apache.storm.StormSubmitter;
//import org.apache.storm.hdfs.bolt.HdfsBolt;
//import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
//import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
//import org.apache.storm.hdfs.bolt.format.FileNameFormat;
//import org.apache.storm.hdfs.bolt.format.RecordFormat;
//import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
//import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
//import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.BasicOutputCollector;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.topology.base.BaseBasicBolt;
//import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.topology.base.BaseWindowedBolt;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//import org.apache.storm.windowing.TupleWindow;
//import org.json.JSONObject;
//
//import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;
//
///**
// * This is a basic example of a Storm topology.
// */
//public class StormBenchmark {
//
//    public static class DeserializeBolt extends BaseRichBolt {
//        OutputCollector _collector;
//
//        @Override
//        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//            _collector = collector;
//        }
//
//        @Override
//        public void execute(Tuple tuple) {
//            JSONObject obj = new JSONObject(tuple.getString(0));
//            String geo = obj.getJSONObject("t").getString("geo");
//            Double price = obj.getJSONObject("m").getDouble("price");
//            _collector.emit(tuple, new Values(
//                    geo,
//                    System.nanoTime(),
//                    price,
//                    price
//            ));
//            _collector.ack(tuple);
//        }
//
//        @Override
//        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields("geo", "ts", "max_price", "min_price"));
//        }
//    }
//
//
//    public static class SlidingWindowAvgBolt extends BaseWindowedBolt {
//
//        private Double sum = 0.0;
//        private OutputCollector collector;
//
//        @Override
//        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//            this.collector = collector;
//        }
//
//        @Override
//        public void execute(TupleWindow inputWindow) {
//            /*
//             * The inputWindow gives a view of
//             * (a) all the events in the window
//             * (b) events that expired since last activation of the window
//             * (c) events that newly arrived since last activation of the window
//             */
//            List<Tuple> tuplesInWindow = inputWindow.get();
//            List<Tuple> newTuples = inputWindow.getNew();
//            List<Tuple> expiredTuples = inputWindow.getExpired();
//
//            /*
//             * Instead of iterating over all the tuples in the window to compute
//             * the sum, the values for the new events are added and old events are
//             * subtracted. Similar optimizations might be possible in other
//             * windowing computations.
//             */
//            Long ts = 0L;
//            for (Tuple tuple : newTuples) {
//                sum += (Double) tuple.getValue(10);
//                if ((Long) tuple.getValue(0) > ts) {
//                    ts = (Long) tuple.getValue(0);
//                }
//            }
//            for (Tuple tuple : expiredTuples) {
//                sum -= (Double) tuple.getValue(10);
//            }
//            Double average = sum / tuplesInWindow.size();
//            collector.emit(new Values(average, ts));
//        }
//
//        @Override
//        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields("average", "timestamp"));
//        }
//    }
//
//    public static class EventProjectBolt extends BaseRichBolt {
//        OutputCollector _collector;
//
//        @Override
//        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//            _collector = collector;
//        }
//
//        @Override
//        public void execute(Tuple tuple) {
//            Long ts = System.nanoTime() -  (Long) tuple.getValue(1);
//            _collector.emit(tuple, new Values(ts));
//            _collector.ack(tuple);
//        }
//
//        @Override
//        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields( "timestamp"));
//        }
//    }
//
//
//
//    public static class EventPrintBolt extends BaseRichBolt {
//        OutputCollector _collector;
//
//        @Override
//        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//            _collector = collector;
//        }
//
//        @Override
//        public void execute(Tuple tuple) {
//            try {
//                File file = new File("/Users/jeka01/Documents/workspaces/benchmarking/streaming-benchmarks-master/output/storm/storm.txt");
//                if (file.exists()) {
//                    file.createNewFile();
//                }
//                FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
//                BufferedWriter bw = new BufferedWriter(fw);
//                bw.write(tuple.getValue(0) + "\n");
//                bw.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            _collector.emit(tuple, new Values(tuple.getValue(0)));
//            _collector.ack(tuple);
//        }
//
//        @Override
//        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields( "timestamp"));
//        }
//    }
//
//    private static HdfsBolt createSink() {
//        RecordFormat format = new DelimitedRecordFormat()
//                .withFieldDelimiter("|");
//
//        // sync the filesystem after every 1k tuples
//        SyncPolicy syncPolicy = new CountSyncPolicy(5);
//
//        // rotate files when they reach 5MB
//        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
//
//        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
//                .withPath("/Users/jeka01/Documents/workspaces/benchmarking/streaming-benchmarks-master/output/storm/");
//
//        HdfsBolt bolt = new HdfsBolt()
//                .withFsUrl("localhost")
//                .withFileNameFormat(fileNameFormat)
//                .withRecordFormat(format)
//                .withRotationPolicy(rotationPolicy)
//                .withSyncPolicy(syncPolicy);
//        return bolt;
//
//    }
//
////    public static class HelloWorldBolt extends BaseBasicBolt {
////
////        @Override
////        public void execute(Tuple tuple, BasicOutputCollector collector) {
////            String msg = tuple.getString(0);
////            System.out.println("=====before write file=====");
////            try {
////                File file = new File("/Users/jeka01/Documents/workspaces/benchmarking/streaming-benchmarks-master/output/storm/storm.txt");
////                if (!file.exists()) {
////                    file.createNewFile();
////                }
////                FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
////                BufferedWriter bw = new BufferedWriter(fw);
////                bw.write(msg + "\n");
////                bw.close();
////            } catch (IOException e) {
////                e.printStackTrace();
////            }
////            System.out.println("=====after write file=====");
////            collector.emit(new Values(msg + " World"));
////        }
////
////        @Override
////        public void declareOutputFields(OutputFieldsDeclarer declarer) {
////            declarer.declare(new Fields("world"));
////        }
////    }
////
//
//
//    public static void main(String[] args) throws Exception {
//
//        if (args.length != 2) {
//            throw new Exception("2 arguments is needed: Config file path and running mode (local or cluster)");
//        }
//        String confPath = args[0];
//        String runningMode = args[1];
//        TopologyBuilder builder = new TopologyBuilder();
//
//        YamlReader reader = new YamlReader(new FileReader(confPath));
//        Object object = reader.read();
//        Map commonConfig = (Map) object;
//
//        int workers = new Integer(commonConfig.get("storm.workers").toString());
//        int ackers = new Integer(commonConfig.get("storm.ackers").toString());
//        int cores = new Integer(commonConfig.get("process.cores").toString());
//        int parallel = Math.max(1, cores / 7);
//        int slideWindowLength = new Integer(commonConfig.get("slidingwindow.length").toString());
//        int slideWindowSlide = new Integer(commonConfig.get("slidingwindow.slide").toString());
//
//        Integer dataGeneratorPort = new Integer(commonConfig.get("datasourcesocket.port").toString());
//        String dataGeneratorHost = InetAddress.getLocalHost().getHostName();
//        Integer benchmarkingTime = new Integer(commonConfig.get("benchmarking.time").toString());
//
//        builder.setSpout("ads", new SocketReceiver(dataGeneratorHost, dataGeneratorPort), 1);
//        builder.setBolt("event_deserializer", new DeserializeBolt(), parallel).shuffleGrouping("ads");
//        builder.setBolt("sliding_avg", new SlidingWindowAvgBolt()
//                .withWindow(new Duration(slideWindowLength, TimeUnit.MILLISECONDS), new Duration(slideWindowSlide, TimeUnit.MILLISECONDS))).shuffleGrouping("event_deserializer");
//        builder.setBolt("event_filter", new EventProjectBolt(), parallel).shuffleGrouping("sliding_avg");
//        //builder.setBolt("hdfsbolt", createSink(), parallel).shuffleGrouping("event_filter");
//        builder.setBolt("fileBolt", new EventPrintBolt(), parallel).shuffleGrouping("event_filter");
//
//        DataGenerator.generate(dataGeneratorPort,benchmarkingTime);
//        Thread.sleep(1000);
//
//        Config conf = new Config();
//        if (runningMode.equals("cluster")) {
//            conf.setNumWorkers(workers);
//            conf.setNumAckers(ackers);
//            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
//        } else if (runningMode.equals("local")) {
//
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("test", conf, builder.createTopology());
////            backtype.storm.utils.Utils.sleep(10000);
////            cluster.killTopology("test");
////            cluster.shutdown();
//        } else {
//            throw new Exception("Second commandline argument should be local or cluster");
//        }
//    }
//
//}
