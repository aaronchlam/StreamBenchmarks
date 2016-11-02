/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import com.esotericsoftware.yamlbeans.YamlReader;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.WriteFormatAsCsv;
import org.apache.flink.streaming.api.functions.sink.WriteSinkFunctionByMillis;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

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
        HashMap conf = (HashMap) object;

        String benchmarkUseCase = conf.get("benchmarking.usecase").toString();
	    Long flushRate = new Long (conf.get("flush.rate").toString());

        //TODO parametertool, checkpoint flush rate, kafka zookeeper configurations

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(flushRate);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        if (benchmarkUseCase.equals("KeyedWindowedAggregation")) {
            keyedWindowedAggregationBenchmark(env, conf);
        } else if (benchmarkUseCase.equals("WindowedJoin")){
            windowedJoin(env, conf);
        }

        else {
            throw new Exception("Please specify use-case name");
        }

        env.execute();

    }







    private static void windowedJoin(StreamExecutionEnvironment env, HashMap conf){
        int slideWindowLength = new Integer(conf.get("slidingwindow.length").toString());
        int slideWindowSlide = new Integer(conf.get("slidingwindow.slide").toString());
        Long flushRate = new Long (conf.get("flush.rate").toString());
        ArrayList<String> hosts = (ArrayList<String>) conf.get("datasourcesocket.hosts");
        Integer port = new Integer(conf.get("datasourcesocket.port").toString());

        DataStream<String> joinStream1 = null;
        DataStream<String> joinStream2 = null;

        for (int i = 0; i < hosts.size(); i++) {
            String host = hosts.get(i);
            DataStream<String> socketSource_i = env.socketTextStream(host, port);
            if (i % 2 == 0) {
                joinStream1 = joinStream1 == null ? socketSource_i : joinStream1.union(socketSource_i);
            } else {
                joinStream2 = joinStream2 == null ? socketSource_i : joinStream2.union(socketSource_i);
            }
        }

        KeyedStream<Tuple3<String, Long, Double>,String> projectedStream1 = joinStream1.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                JSONObject obj = new JSONObject(s);
                String geo = obj.getString("geo");
                Double price = obj.getDouble("price");
                Long ts = obj.has("ts") ? obj.getLong("ts"):System.currentTimeMillis();
                return new Tuple3<String, Long, Double>(geo, ts , price);
            }
        }).keyBy(new KeySelector<Tuple3<String,Long,Double>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Double> t) throws Exception {
                return t.f0;
            }
        });

        KeyedStream<Tuple3<String, Long, Double>,String> projectedStream2 = joinStream2.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                JSONObject obj = new JSONObject(s);
                String geo = obj.getString("geo");
                Double price = obj.getDouble("price");
                Long ts = obj.has("ts") ? obj.getLong("ts"):System.currentTimeMillis();
                return new Tuple3<String, Long, Double>(geo, ts , price);
            }
        }).keyBy(new KeySelector<Tuple3<String, Long, Double>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Double> t) throws Exception {
                return t.f0;
            }
        });


        DataStream<Tuple3<String, Long, Double>> joinedStream = projectedStream1.join(projectedStream2).
                where(new KeySelector<Tuple3<String, Long, Double>, String>() {

                    @Override
                    public String getKey(Tuple3<String, Long, Double> tuple) throws Exception {
                        return tuple.f0;
                    }
                }).
                equalTo(new KeySelector<Tuple3<String, Long, Double>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Long, Double> tuple) throws Exception {
                        return tuple.f0;
                    }
                }).
                window(SlidingProcessingTimeWindows.of(Time.milliseconds(slideWindowLength), Time.milliseconds(slideWindowSlide)))
                .apply(new JoinFunction<Tuple3<String, Long, Double>, Tuple3<String, Long, Double>, Tuple3<String, Long, Double>>() {

                    @Override
                    public Tuple3<String, Long, Double> join(Tuple3<String, Long, Double> t1, Tuple3<String, Long, Double> t2) throws Exception {
                        return new Tuple3<String, Long, Double>(t1.f0, Math.max(t1.f1,t2.f1), t1.f2<0 ||t2.f2<0? -100D : Math.abs(t1.f2-t2.f2));
                    }
                });


        DataStream<Tuple4<String, Long, Double, Long>> resultingStream = joinedStream.map(new MapFunction<Tuple3<String, Long, Double>, Tuple4<String, Long, Double, Long>>() {
            @Override
            public Tuple4<String, Long, Double, Long> map(Tuple3<String, Long, Double> t1) throws Exception {
                return new Tuple4<String, Long, Double, Long>(t1.f0, System.currentTimeMillis()  - t1.f1, t1.f2, t1.f1);
            }
        });

        String outputFile = conf.get("flink.output").toString();
        resultingStream.addSink(new WriteSinkFunctionByMillis<Tuple4<String, Long, Double, Long>>(outputFile, new WriteFormatAsCsv(), flushRate));

    }










    private static void keyedWindowedAggregationBenchmark(StreamExecutionEnvironment env, HashMap conf){
        int slideWindowLength = new Integer(conf.get("slidingwindow.length").toString());
        int slideWindowSlide = new Integer(conf.get("slidingwindow.slide").toString());
        Long flushRate = new Long (conf.get("flush.rate").toString());
        ArrayList<String> hosts = (ArrayList<String>) conf.get("datasourcesocket.hosts");
        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        DataStream<String> socketSource = null;
        for (String host : hosts) {
            DataStream<String> socketSource_i = env.socketTextStream(host, port);
            socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
        }

        DataStream<Tuple5<String, Long, Double, Double,Long>> messageStream = socketSource.map(new MapFunction<String, Tuple5<String, Long, Double, Double,Long>>() {
                    @Override
                    public Tuple5<String, Long, Double, Double,Long> map(String s) throws Exception {
                        JSONObject obj = new JSONObject(s);
                        String geo = obj.getString("geo");
                        Double price = obj.getDouble("price");
                        Long ts = obj.has("ts") ? obj.getLong("ts"):System.currentTimeMillis();
                        return new Tuple5<String, Long, Double, Double,Long>(geo, ts , price, price,1L);
                    }
                });

        DataStream<Tuple5<String, Long, Double, Double,Long>> aggregatedStream = messageStream.keyBy(0)
                .timeWindow(Time.milliseconds(slideWindowLength), Time.milliseconds(slideWindowSlide)).
                        reduce(new ReduceFunction<Tuple5<String, Long, Double, Double,Long>>() {
                            @Override
                            public Tuple5<String, Long, Double, Double,Long> reduce(Tuple5<String, Long, Double, Double,Long> t1, Tuple5<String, Long, Double, Double,Long> t2) throws Exception {
                                Double maxPrice = Math.max(t1.f2, t2.f2);
                                Double minPrice = Math.min(t1.f3, t2.f3);
                                Long ts = Math.max(t1.f1, t2.f1);
                                Long windowElements = t1.f4 + t2.f4;
                                return new Tuple5<String, Long, Double, Double,Long>(t1.f0, ts, maxPrice, minPrice,windowElements);
                            }
                        });


        DataStream<Tuple6<String, Long, Double, Double,Long,Long>> mappedStream = aggregatedStream.map(new MapFunction<Tuple5<String, Long, Double, Double,Long>, Tuple6<String, Long, Double, Double,Long, Long>>() {
            @Override
            public Tuple6<String, Long, Double, Double,Long,Long> map(Tuple5<String, Long, Double, Double,Long> t1) throws Exception {
                return new Tuple6<String, Long, Double, Double,Long, Long>(t1.f0, System.currentTimeMillis()  - t1.f1, t1.f2, t1.f3,t1.f4, t1.f1);
            }
        });


        String outputFile = conf.get("flink.output").toString();
        int batchSize = new Integer(conf.get("flink.outputbatchsize").toString());
        RollingSink sink = new RollingSink<String>(outputFile);
        sink.setBatchSize(1024 * batchSize); // this is 400 MB,

        mappedStream.addSink(sink);


    }
}


