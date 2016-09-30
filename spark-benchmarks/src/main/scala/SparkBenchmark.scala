/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

package spark.benchmark

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import benchmark.common.Utils
import data.source.socket.DataGenerator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Minutes, Seconds, StreamingContext}
import org.json.JSONObject


object SparkBenchmark {
  def main(args: Array[String]) {
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];
    val batchSize = commonConfig.get("spark.batchtime") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }

   val master = commonConfig.get("spark.master").toString
    val sparkConf = new SparkConf().setAppName("KafkaRedisAdvertisingStream").setMaster(master)
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))
    //val textSource = ssc.textFileStream("hdfs://...")

    val dataGeneratorHost = InetAddress.getLocalHost().getHostName()
    val dataGeneratorPort = commonConfig.get("datasourcesocket.port").toString().toInt

    val slidingWindowLength = commonConfig.get("slidingwindow.length").toString().toInt
    val slidingWindowSlide = commonConfig.get("slidingwindow.slide").toString().toInt


    val socketDataSource = ssc.receiverStream(new SocketReceiver(dataGeneratorHost,dataGeneratorPort))

    val keyedStream = socketDataSource.map(s => extractTuples(s))
      // use case begins here
    //val keyedStream = tupleStream.transform(rdd => rdd.keyBy(_._1).groupByKey()   )

    val aggregatedStream = keyedStream.groupByKeyAndWindow(Milliseconds(slidingWindowLength),Milliseconds(slidingWindowSlide))
        .map(window=>minMaxTuplesRDD(window))
      //.foreachRDD(rdd=>{rdd.foreach(print);print(rdd.keys); rdd})
                                       //   .foreachRDD(rdd=>{rdd.foreach(print) ; println("ended here ")})


    
    //use case ends here

    val resultStream = aggregatedStream.map(tuple =>  new Tuple4[String, Long, Double, Double](tuple._1, System.currentTimeMillis() - tuple._2, tuple._3, tuple._4) )
    val outputFile = commonConfig.get("spark.output").toString
    resultStream.saveAsTextFiles(outputFile);

    val warmupCount: Long = commonConfig.get("warmup.count").toString.toLong
    val benchmarkingCount: Long = commonConfig.get("benchmarking.count").toString.toLong
    val sleepTime: Long = commonConfig.get("datagenerator.sleep").toString.toLong

    DataGenerator.generate(dataGeneratorPort, benchmarkingCount, warmupCount, sleepTime);
    Thread.sleep(1000L)

    ssc.start()

    ssc.awaitTermination()
  }

  def extractTuples(s: String): ((String), (Long, Double, Double)) = {
    val obj: JSONObject = new JSONObject(s)

    val price: Double = obj.getJSONObject("m").getDouble("price")
    val geo: String = obj.getJSONObject("t").getString("geo")
    return ((geo),( System.currentTimeMillis(),price, price))
  }

  def minMaxTuples (t1: (String, Long, Double, Double) ,
                 t2: (String, Long, Double, Double)) : (String, Long, Double, Double) = {
    val maxPrice: Double = Math.max(t1._3, t2._3)
    val minPrice: Double = Math.min(t1._4, t2._4)
    val ts: Long = Math.max(t1._2, t2._2)
    return new Tuple4[String, Long, Double, Double](t1._1, ts, maxPrice, minPrice)
  }

  def minMaxTuplesRDD (window: (String, Iterable[(Long, Double, Double)] )) : (String, Long, Double, Double) = {
      val maxPrice = window._2.toArray.maxBy(_._2)._2
      val minPrice = window._2.toArray.minBy(_._3)._3
      val maxTS = window._2.toArray.maxBy(_._1)._1
      return (window._1,maxTS, maxPrice, minPrice)
  }
}
