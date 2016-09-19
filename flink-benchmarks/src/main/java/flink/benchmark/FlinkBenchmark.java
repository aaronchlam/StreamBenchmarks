/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEvent;
import data.source.socket.DataGenerator;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-FlinkBenchmark.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class FlinkBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkBenchmark.class);


    public static void main(final String[] args) throws Exception {

        if (args == null || args.length != 2) {
            throw new Exception("configuration file parameter is needed. Ex: --confPath ../conf/benchmarkConf.yaml");
        }

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String confFilePath = parameterTool.getRequired("confPath");
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        Map conf = (Map) object;

        int hosts = new Integer(conf.get("process.hosts").toString());
        int cores = new Integer(conf.get("process.cores").toString());

        String dataGeneratorHost = InetAddress.getLocalHost().getHostName();
        Integer dataGeneratorPort = new Integer(conf.get("datasourcesocket.port").toString());
        int slideWindowLength = new Integer(conf.get("slidingwindow.length").toString());
        int slideWindowSlide = new Integer(conf.get("slidingwindow.slide").toString());

        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

        // Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));

        if (flinkBenchmarkParams.has("flink.checkpoint-interval")) {
            // enable checkpointing for fault tolerance
            env.enableCheckpointing(flinkBenchmarkParams.getLong("flink.checkpoint-interval", 1000));
        }
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        env.setParallelism(hosts * cores);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<Tuple4<String, Long, Double, Double>> messageStream =
                env.socketTextStream(dataGeneratorHost, dataGeneratorPort).map(new MapFunction<String, Tuple4<String, Long, Double, Double>>() {
                    @Override
                    public Tuple4<String, Long, Double, Double> map(String s) throws Exception {
                        JSONObject obj = new JSONObject(s);
                        String geo = obj.getJSONObject("t").getString("geo");
                        Double price = obj.getJSONObject("m").getDouble("price");

                        return new Tuple4<String, Long, Double, Double>(geo, System.nanoTime(), price, price);
                    }
                });


        // do some stuff with use case


        DataStream<Tuple4<String, Long, Double, Double>> aggregatedStream = messageStream.keyBy(0).timeWindow(Time.milliseconds(slideWindowLength), Time.milliseconds(slideWindowSlide)).reduce(new ReduceFunction<Tuple4<String, Long, Double, Double>>() {
            @Override
            public Tuple4<String, Long, Double, Double> reduce(Tuple4<String, Long, Double, Double> t1, Tuple4<String, Long, Double, Double> t2) throws Exception {
                Double maxPrice = Math.max(t1.f2, t2.f2);
                Double minPrice = Math.min(t1.f3, t2.f3);
                Long ts = Math.max(t1.f1, t2.f1);
                return new Tuple4<String, Long, Double, Double>(t1.f0, ts, maxPrice, minPrice);
            }
        });


        // use case ends here


        DataStream<Tuple4<String, Long, Double, Double>> resultingStream = aggregatedStream.map(new MapFunction<Tuple4<String, Long, Double, Double>, Tuple4<String, Long, Double, Double>>() {
            @Override
            public Tuple4<String, Long, Double, Double> map(Tuple4<String, Long, Double, Double> t1) throws Exception {
                return new Tuple4<String, Long, Double, Double>(t1.f0, System.nanoTime() - t1.f1, t1.f2, t1.f3);
            }
        });

        String outputFile = conf.get("flink.output").toString();

        resultingStream.print();
      //  resultingStream.writeAsCsv(outputFile);

        Long benchmarkingCount = new Long(conf.get("benchmarking.count").toString());
        Long warmupCount = new Long(conf.get("warmup.count").toString());
        Long sleepTime = new Long(conf.get("datagenerator.sleep").toString());

        DataGenerator.generate(dataGeneratorPort, benchmarkingCount, warmupCount, sleepTime);
        Thread.sleep(1000L);
        env.execute();
    }


    private static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<String, String>();
        flinkConfs.put("topic", getKafkaTopic(conf));
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("group.id", "myGroup");

        return flinkConfs;
    }

    private static String getZookeeperServers(Map conf) {
        if (!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if (!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if (!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map conf) {
        if (!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String) conf.get("kafka.topic");
    }


    public static String listOfStringToString(List<String> list, String port) {
        String val = "";
        for (int i = 0; i < list.size(); i++) {
            val += list.get(i) + ":" + port;
            if (i < list.size() - 1) {
                val += ",";
            }
        }
        return val;
    }

    class EventExtractor implements AssignerWithPeriodicWatermarks<Tuple10<Long, Long, String, String, String, String, String, String, String, String>> {

        @Override
        public Watermark getCurrentWatermark() {
            return null;
        }

        @Override
        public long extractTimestamp(Tuple10<Long, Long, String, String, String, String, String, String, String, String> longLongStringStringStringStringStringStringStringStringTuple10, long l) {
            return 0;
        }
    }


}




