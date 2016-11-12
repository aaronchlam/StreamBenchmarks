/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.CommonConfig;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        CommonConfig.initializeConfig(confFilePath);

        //TODO parametertool, checkpoint flush rate, kafka zookeeper configurations

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(CommonConfig.FLUSH_RATE());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        if (CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.AGGREGATION_USECASE)) {
            keyedWindowedAggregationBenchmark(env);
        } else if (CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.JOIN_USECASE)){
            windowedJoin(env);
        } else if(CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.DUMMY_CONSUMER)){
            dummyConsumer(env);
        }

        else {
            throw new Exception("Please specify use-case name");
        }

        env.execute();

    }


    private static void dummyConsumer(StreamExecutionEnvironment env){
        DataStream<String> socketSource = null;
        for (String host : CommonConfig.DATASOURCE_HOSTS()) {
            for (int port: CommonConfig.DATASOURCE_PORTS()){
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }
        DataStream<String> filteredStream = socketSource.filter(t->false);
        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        filteredStream.addSink(sink);


    }



    private static void windowedJoin(StreamExecutionEnvironment env){
        DataStream<String> joinStream1 = null;
        DataStream<String> joinStream2 = null;

        for (String host: CommonConfig.DATASOURCE_HOSTS()) {
            for (int i = 0; i < CommonConfig.DATASOURCE_PORTS().size(); i++){
                int port = CommonConfig.DATASOURCE_PORTS().get(i);
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                if (i % 2 == 0) {
                    joinStream1 = joinStream1 == null ? socketSource_i : joinStream1.union(socketSource_i);
                } else {
                    joinStream2 = joinStream2 == null ? socketSource_i : joinStream2.union(socketSource_i);
                }
            }
        }

        DataStream<Tuple3<String, Long, Double>> projectedStream1 = joinStream1.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                JSONObject obj = new JSONObject(s);
                String geo = obj.getString("geo");
                Double price = obj.getDouble("price");
                Long ts =  obj.getLong("ts");
                return new Tuple3<String, Long, Double>(geo, ts , price);
            }
        });

        DataStream<Tuple3<String, Long, Double>> projectedStream2 = joinStream2.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                JSONObject obj = new JSONObject(s);
                String geo = obj.getString("geo");
                Double price = obj.getDouble("price");
                Long ts =  obj.getLong("ts");
                return new Tuple3<String, Long, Double>(geo, ts , price);
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
                window(SlidingProcessingTimeWindows.of(Time.milliseconds(CommonConfig.SLIDING_WINDOW_LENGTH()), Time.milliseconds(CommonConfig.SLIDING_WINDOW_SLIDE())))
                .apply(new JoinFunction<Tuple3<String, Long, Double>, Tuple3<String, Long, Double>, Tuple3<String, Long, Double>>() {

                    @Override
                    public Tuple3<String, Long, Double> join(Tuple3<String, Long, Double> t1, Tuple3<String, Long, Double> t2) throws Exception {
                        return new Tuple3<String, Long, Double>(t1.f0, Math.max(t1.f1,t2.f1),  Math.abs(t1.f2-t2.f2));
                    }
                });


        DataStream<Tuple4<String, Long, Double, Long>> resultingStream = joinedStream.map(new MapFunction<Tuple3<String, Long, Double>, Tuple4<String, Long, Double, Long>>() {
            @Override
            public Tuple4<String, Long, Double, Long> map(Tuple3<String, Long, Double> t1) throws Exception {
                return new Tuple4<String, Long, Double, Long>(t1.f0, System.currentTimeMillis()  - t1.f1, t1.f2, t1.f1);
            }
        });


        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        resultingStream.addSink(sink);

    }




    private static void keyedWindowedAggregationBenchmark(StreamExecutionEnvironment env){
        DataStream<String> socketSource = null;
        for (String host : CommonConfig.DATASOURCE_HOSTS()) {
            for (Integer port: CommonConfig.DATASOURCE_PORTS()){
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }

        DataStream<Tuple5<String, Long, Double, Integer,Integer>> messageStream = socketSource.map(new MapFunction<String, Tuple5<String, Long, Double, Integer,Integer>>() {
                    @Override
                    public Tuple5<String, Long, Double, Integer,Integer> map(String s) throws Exception {
                        JSONObject obj = new JSONObject(s);
                        String geo = obj.getString("geo");
                        Double price = obj.getDouble("price");
                        Long ts =  obj.getLong("ts");
                        return new Tuple5<String, Long, Double, Integer,Integer>(geo, ts , price, 1 ,1);
                    }
                });

        DataStream<Tuple5<String, Long, Double, Integer,Integer>> aggregatedStream = messageStream.keyBy(0)
                .timeWindow(Time.milliseconds(CommonConfig.SLIDING_WINDOW_LENGTH()), Time.milliseconds(CommonConfig.SLIDING_WINDOW_SLIDE())).
                        reduce(new ReduceFunction<Tuple5<String, Long, Double, Integer,Integer>>() {
                            @Override
                            public Tuple5<String, Long, Double, Integer,Integer> reduce(Tuple5<String, Long, Double, Integer,Integer> t1, Tuple5<String, Long, Double, Integer,Integer> t2) throws Exception {
                                Double avgPrice = (t1.f3 * t1.f2 + t2.f3 * t2.f2) / t1.f3 + t2.f3;
                                Integer avgCount = t1.f3 + t2.f3;
                                Integer windowCount = t1.f4 + t2.f4;
                                Long ts = Math.max(t1.f1, t2.f1);
                                return new Tuple5<String, Long, Double, Integer,Integer>(t1.f0, ts, avgPrice, avgCount,windowCount);
                            }
                        });


        DataStream<Tuple5<String, Long, Double,Integer,Long>> mappedStream = aggregatedStream.map(new MapFunction<Tuple5<String, Long, Double, Integer, Integer>, Tuple5<String, Long, Double, Integer, Long>>() {
            @Override
            public Tuple5<String, Long, Double, Integer, Long> map(Tuple5<String, Long, Double, Integer, Integer> t1) throws Exception {
                return new Tuple5<String, Long, Double,Integer,Long>(t1.f0, System.currentTimeMillis()  - t1.f1, t1.f2,t1.f4, t1.f1);
            }
        });


        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        mappedStream.addSink(sink);


    }
}


