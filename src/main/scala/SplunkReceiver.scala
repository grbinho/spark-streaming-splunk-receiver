package org.apache.spark.streaming.receiver

import com.splunk._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.progress.{ProgressStore, TimeProgressRecord}
import org.joda.time.{DateTime, Seconds}
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class SplunkReceiver(host: String,
                     port: Int,
                     username: String,
                     password: String,
                     searchQuery: String,
                     progressTracker: ProgressStore,
                     // User can decide that he/she will commit progress only when additional processing was completed,
                     // so actual progress can be written downstream.
                     saveProgress: Boolean,
                     startTime: DateTime)
  extends Receiver[Event](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  val SPLUNK_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

  def onStart(): Unit = {
    new Thread("Splunk Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop(): Unit = {

  }

  /** Accepted splunk date time format
   */
  private def toSplunkStringFormat(value: DateTime) = value.toString(SPLUNK_TIME_FORMAT)
  private def fromSplunkStringFormat(value: String) = DateTime.parse(value, DateTimeFormat.forPattern(SPLUNK_TIME_FORMAT))

  private def getQueryWindow(differenceInSecondsFromNow: Int, requestDurationSeconds: Long, previousQueryWindowSeconds: Int) : Int = {
    var newQueryWindow: Int = differenceInSecondsFromNow match {
      case diff if diff > 3600 => 3600
      case diff if diff > 1800 => 900
      case diff if diff > 900 => 600
      case diff if diff > 300 => 300
      case _ => differenceInSecondsFromNow
    }

    newQueryWindow = Math.max(1,2 * previousQueryWindowSeconds - requestDurationSeconds.toInt)

    newQueryWindow
  }

  private def receive(): Unit = {

    logInfo(s"Starting Splunk receiver")

    var queryWindowSeconds = 10

    var queryStartTime: DateTime = startTime //TODO: Timezone??
    var queryEndTime: DateTime = startTime

    progressTracker.open()

    // Take the later value
    val progressValue = progressTracker.read()
    if(!progressValue.isEmpty) {
      val progressValueTime = fromSplunkStringFormat(progressValue)
      if(progressValueTime.isAfter(startTime)) queryStartTime = progressValueTime
    }

    queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)

    logInfo(s"Initial period start: $queryStartTime, end: $queryEndTime")

    try {
      val loginArgs: ServiceArgs = new ServiceArgs
      loginArgs.setUsername(username)
      loginArgs.setPassword(password)
      loginArgs.setHost(host)
      loginArgs.setPort(port)
      loginArgs.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2)

      val service = Service.connect(loginArgs)

      //Run forever since start time and then query every x seconds
      while(!isStopped)
      {
        val exportArgs = new JobExportArgs
        exportArgs.setEarliestTime(toSplunkStringFormat(queryStartTime)) //Inclusive
        exportArgs.setLatestTime(toSplunkStringFormat(queryEndTime)) //Exclusive
        exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL)
        exportArgs.setOutputMode(JobExportArgs.OutputMode.JSON)
        exportArgs.setOutputTimeFormat("%Y-%m-%d %H:%M:%S.%3N %:z")

        // Run the search with a search query and export arguments
        val exportSearch = service.export(searchQuery, exportArgs)
        // Display results using the SDK's multi-results reader for XML
        val multiResultsReader = new MultiResultsReaderJson(exportSearch)

        val receiveStartTime = System.nanoTime()

        val events = ArrayBuffer[Event]()
        multiResultsReader.foreach(searchResult =>
          searchResult.foreach(event =>
            events += event))

        store(events)

        val receiveEndTime = System.nanoTime()

        multiResultsReader.close()

        val requestDuration = (receiveEndTime - receiveStartTime) / 1000000000 //nano seconds -> seconds
        logInfo(s"Splunk REQUEST DURATION: $requestDuration s")

        if(saveProgress) {
          val progressRecord = new TimeProgressRecord(queryEndTime)
          progressTracker.writeProgress(progressRecord)
        }


        logInfo(s"CURRENT TIME WINDOW: $queryWindowSeconds s")
        val differenceFromNow = Seconds.secondsBetween(queryEndTime, DateTime.now).getSeconds
        queryWindowSeconds = getQueryWindow(differenceFromNow, requestDuration, queryWindowSeconds)
        logInfo(s"NEW TIME WINDOW: $queryWindowSeconds s")
        queryStartTime = queryEndTime
        queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)
        if(queryEndTime.isAfter(DateTime.now)) queryEndTime = DateTime.now

        //DO NOT QUERY IF THERE IS NOTHING TO QUERY
        //TODO: Get some back pressure information from spark??
        if(events.length == 0 && differenceFromNow < 10) {
          logInfo(s"No events, sleeping for one second.")
          Thread.sleep(1000)
        }

        logInfo(s"Next period start: $queryStartTime, end: $queryEndTime")
      }

      restart("Trying to connect again")
    } catch {
      case e: Throwable => restart("Error receiving data from Splunk", e)
    }
  }
}
