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
    val host1 = commonConfig.get("datasourcesocket.joinHost1").toString
    val host2 = commonConfig.get("datasourcesocket.joinHost2").toString
    val port = commonConfig.get("datasourcesocket.port").toString().toInt
    val joinSource1: DStream[String] = ssc.receiverStream(new SocketReceiver(host1, port));
    val joinSource2: DStream[String] = ssc.receiverStream(new SocketReceiver(host2, port));

    val windowedStream1 = joinSource1.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getDouble("price")
      val geo: String = obj.getString("geo")
      val ts: Long = if (obj.has("ts")) obj.getLong("ts") else System.currentTimeMillis();
      ((geo), (ts, price))
    }).window(Milliseconds(slidingWindowLength), Milliseconds(slidingWindowSlide))

    val windowedStream2 = joinSource2.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getDouble("price")
      val geo: String = obj.getString("geo")
      val ts: Long = if (obj.has("ts")) obj.getLong("ts") else System.currentTimeMillis();
      ((geo), (ts, price))
    }).window(Milliseconds(slidingWindowLength), Milliseconds(slidingWindowSlide))


    val joinedStream = windowedStream1.join(windowedStream2).map(t => (t._1,
      System.currentTimeMillis() - Math.max(t._2._1._1, t._2._2._1),
      if (t._2._1._2 < 0 || t._2._2._2 < 0) -100D else Math.abs(t._2._1._2 - t._2._2._2),
      Math.max(t._2._1._1, t._2._2._1)))

    val outputFile = commonConfig.get("spark.output").toString
    joinedStream.saveAsTextFiles(outputFile);

  }

  def keyedWindowedAggregationBenchmark(ssc: StreamingContext, commonConfig: java.util.HashMap[String, Any]) = {
    val slidingWindowLength = commonConfig.get("slidingwindow.length").toString().toInt
    val slidingWindowSlide = commonConfig.get("slidingwindow.slide").toString().toInt
    val host: String = commonConfig.get("datasourcesocket.singleHost").toString
    val port = commonConfig.get("datasourcesocket.port").toString().toInt

    val socketDataSource: DStream[String] = ssc.receiverStream(new SocketReceiver(host, port))


    val keyedStream = socketDataSource.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getDouble("price")
      val geo: String = obj.getString("geo")
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
    val mappedStream = windowedStream.map(tuple => new Tuple6[String, Long, Double, Double, Long, Long](tuple._1, System.currentTimeMillis() - tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4, tuple._2._1))

    val outputFile = commonConfig.get("spark.output").toString
    mappedStream.saveAsTextFiles(outputFile);
    // resultStream.print();

  }
}
