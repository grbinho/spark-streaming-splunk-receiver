package main

import com.splunk.Event
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.receiver.SplunkReceiver
import org.apache.spark.streaming.receiver.progress.DfsProgressStore
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._


case class SimpleEvent(vendorName: String, TipPickupDateTime: String)

object SplunkSparkStreaming {
  def main(args: Array[String]): Unit = {

    //Example Spark Streaming application that uses SplunkReceiver
    val spark = SparkSession.builder().appName("SplunkStreaming").getOrCreate()
    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))
    val progressStore = new DfsProgressStore("/tmp/spark_streaming_progress", "splunk_nytaxi")

    val rides = ssc.receiverStream(new SplunkReceiver("localhost", 8089, "export", "export",
      "search index=nytaxi | fields vendor_name, Trip_Pickup_DateTime, _time | fields - _raw, _bkt, _cd, _serial, _subsecond, _si, _sourcetype, _indextime | sort +_time",
      progressStore, true, new DateTime(2011, 8, 10, 11, 59)))


    rides.foreachRDD { (rdd: RDD[Event], time: Time) =>
      val rddEvents = rdd.map(e => SimpleEvent(e.get("vendor_name"), e.get("Trip_Pickup_DateTime")))
      val ridesDF = rddEvents.toDF
      ridesDF.write.parquet(s"/tmp/spark_streaming/nytaxi_${time.milliseconds}.parquet")
    }

    val count = rides.count()
    count.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
