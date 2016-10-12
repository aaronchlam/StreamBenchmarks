/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

package spark.benchmark

import java.io.FileReader
import java.util

import com.esotericsoftware.yamlbeans.YamlReader
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.json.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import scala.collection.JavaConversions._


object SparkBenchmark {
  def main(args: Array[String]) {

    val reader: YamlReader = new YamlReader(new FileReader(args(0)));
    val obj = reader.read();
    val commonConfig: java.util.HashMap[String, Any] = obj.asInstanceOf[java.util.HashMap[String, Any]];
    val batchTime = commonConfig.get("spark.batchtime").toString().toLong

    val master = commonConfig.get("spark.master").toString
    val sparkConf = new SparkConf().setAppName("KafkaRedisAdvertisingStream").setMaster(master)
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchTime))

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    if (commonConfig.get("benchmarking.usecase").toString == "KeyedWindowedAggregation")
      keyedWindowedAggregationBenchmark(ssc, commonConfig);
    else if (commonConfig.get("benchmarking.usecase").toString == "WindowedJoin")
      windowedJoin(ssc, commonConfig);
    else throw new Exception("Please specify use-case name")

    ssc.start()
    ssc.awaitTermination()
  }

  def windowedJoin(ssc: StreamingContext, commonConfig: java.util.HashMap[String, Any]) = {
    val slidingWindowLength = commonConfig.get("slidingwindow.length").toString().toInt
    val slidingWindowSlide = commonConfig.get("slidingwindow.slide").toString().toInt
    val hosts = commonConfig.get("datasourcesocket.hosts").asInstanceOf[util.ArrayList[String]].toList
    val port = commonConfig.get("datasourcesocket.port").toString().toInt
    var joinSource1: DStream[String] = null;
    var joinSource2: DStream[String] = null;
    for (hostIndex <- hosts.indices) {
      val host = hosts.get(hostIndex)
      val socketDataSource_i: DStream[String] = ssc.receiverStream(new SocketReceiver(host, port))
      if (hostIndex % 2 == 1) {
        joinSource1 = if (joinSource1 == null) socketDataSource_i else joinSource1.union(socketDataSource_i)
      }
      else {
        joinSource2 = if (joinSource2 == null) socketDataSource_i else joinSource2.union(socketDataSource_i)
      }
    }

    val windowedStream1 = joinSource1.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getJSONObject("m").getDouble("price")
      val geo: String = obj.getJSONObject("t").getString("geo")
      val ts: Long = if (obj.has("ts")) obj.getLong("ts") else System.currentTimeMillis();
      ((geo), (ts, price))
    }).window(Milliseconds(slidingWindowLength), Milliseconds(slidingWindowSlide))

    val windowedStream2 = joinSource2.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getJSONObject("m").getDouble("price")
      val geo: String = obj.getJSONObject("t").getString("geo")
      val ts: Long = if (obj.has("ts")) obj.getLong("ts") else System.currentTimeMillis();
      ((geo), (ts, price))
    }).window(Milliseconds(slidingWindowLength), Milliseconds(slidingWindowSlide))


    val joinedStream  = windowedStream1.join(windowedStream2).map(t=>(t._1, System.currentTimeMillis() -  Math.max(t._2._1._1, t._2._2._1), if (t._2._1._2 < 0 || t._2._2._2 < 0) -100D else Math.abs(t._2._1._2 - t._2._2._2) ))
    val resultStream = joinedStream.filter(t=> t._3 > 0)

//    val windowReduce1 = windowedStream1.reduceByKey((t1, t2) => new Tuple4[Long, Double, Double, Long](Math.max(t1._1, t2._1), Math.max(t1._2, t2._2), Math.min(t1._3, t2._3), t1._4 + t2._4))
//    val windowReduce2 = windowedStream2.reduceByKey((t1, t2) => new Tuple4[Long, Double, Double, Long](Math.max(t1._1, t2._1), Math.max(t1._2, t2._2), Math.min(t1._3, t2._3), t1._4 + t2._4))
//
//    val joinedStream = windowReduce1
//      .join(windowReduce2)
//      .map(tuple => ((tuple._1), (Math.max(tuple._2._1._1, tuple._2._2._1), tuple._2._1._2 - tuple._2._1._3, tuple._2._2._2 - tuple._2._2._3   , (tuple._2._1._4 + tuple._2._2._4)/2)))
    //.reduceByKey((t1,t2)=> (Math.max(t1._1,t2._1),Math.max(t1._2,t2._2), Math.min(t1._3,t2._3) ))
    val outputFile = commonConfig.get("spark.output").toString
    resultStream.saveAsTextFiles(outputFile);

  }

  def keyedWindowedAggregationBenchmark(ssc: StreamingContext, commonConfig: java.util.HashMap[String, Any]) = {
    val slidingWindowLength = commonConfig.get("slidingwindow.length").toString().toInt
    val slidingWindowSlide = commonConfig.get("slidingwindow.slide").toString().toInt
    val hosts: util.ArrayList[String] = commonConfig.get("datasourcesocket.hosts").asInstanceOf[util.ArrayList[String]]
    val port = commonConfig.get("datasourcesocket.port").toString().toInt
    var socketDataSource: DStream[String] = null;

    for (host <- hosts) {
      val socketDataSource_i: DStream[String] = ssc.receiverStream(new SocketReceiver(host, port))
      socketDataSource = if (socketDataSource == null) socketDataSource_i else socketDataSource.union(socketDataSource_i)
    }


    val keyedStream = socketDataSource.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getJSONObject("m").getDouble("price")
      val geo: String = obj.getJSONObject("t").getString("geo")
      val ts: Long = if (obj.has("ts")) obj.getLong("ts") else System.currentTimeMillis();

      ((geo), (ts, price, price, 1L))
    })

    val windowedStream = keyedStream.window(Milliseconds(slidingWindowLength), Milliseconds(slidingWindowSlide))
      .reduceByKey((t1, t2) => {
        val maxPrice: Double = Math.max(t1._2, t2._2)
        val minPrice: Double = Math.min(t1._3, t2._3)
        val ts: Long = Math.max(t1._1, t2._1)
        val elementCount: Long = t1._4 + t2._4
        new Tuple4[Long, Double, Double, Long](ts, maxPrice, minPrice, elementCount)
      })
    val resultStream = windowedStream.map(tuple => new Tuple5[String, Long, Double, Double, Long](tuple._1, System.currentTimeMillis() - tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4))

    val outputFile = commonConfig.get("spark.output").toString
    resultStream.saveAsTextFiles(outputFile);
    // resultStream.print();

  }
}
