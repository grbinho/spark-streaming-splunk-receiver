package main

import io.grba.spark.streaming.receiver.SplunkReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import scala.collection.JavaConversions._

object SplunkSparkStreaming {
  def main(args: Array[String]): Unit = {

    //Example Spark Streaming application that uses SplunkReceiver

    val sparkConf = new SparkConf().setAppName("SplunkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val rides = ssc.receiverStream(new SplunkReceiver("localhost", 8089, "export", "export",
      "search index=nytaxi | fields vendor_name, Trip_Pickup_DateTime, _time | fields - _raw, _bkt, _cd, _serial, _subsecond, _si, _sourcetype, _indextime",
      new DateTime(2011, 8, 10, 11, 59), 10))

    val mappedRides = rides.map(e => {
      var acc = ""
      e.keySet().foreach(k => acc += e.get(k) + ",")
      acc
    })




    val count = mappedRides.count()

    count.print()
    mappedRides.print(100)


    ssc.start()
    ssc.awaitTermination()


  }
}
