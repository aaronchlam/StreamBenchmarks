/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

package spark.benchmark

import java.io.FileReader

import com.esotericsoftware.yamlbeans.YamlReader
import data.source.socket.DataGenerator
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Minutes, Seconds, StreamingContext}
import org.json.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream


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

    val dataGeneratorHost = commonConfig.get("datasourcesocket.host").toString()
    val dataGeneratorPort = commonConfig.get("datasourcesocket.port").toString().toInt
    val socketDataSource = ssc.receiverStream(new SocketReceiver(dataGeneratorHost, dataGeneratorPort))

    if (commonConfig.get("benchmarking.usecase").toString == "KeyedWindowedAggregation")
      keyedWindowedAggregationBenchmark(socketDataSource, commonConfig);


    DataGenerator.generate(commonConfig);
    Thread.sleep(1000L)

    ssc.start()
    ssc.awaitTermination()
  }


  def keyedWindowedAggregationBenchmark(socketDataSource: DStream[String], commonConfig: java.util.HashMap[String, Any]) = {
    val slidingWindowLength = commonConfig.get("slidingwindow.length").toString().toInt
    val slidingWindowSlide = commonConfig.get("slidingwindow.slide").toString().toInt


    val keyedStream = socketDataSource.map(s => {
      val obj: JSONObject = new JSONObject(s)
      val price: Double = obj.getJSONObject("m").getDouble("price")
      val geo: String = obj.getJSONObject("t").getString("geo")
      val ts: Long = if (obj.has("ts"))  obj.getLong("ts") else System.currentTimeMillis() ;

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
    resultStream.print();

  }
}
