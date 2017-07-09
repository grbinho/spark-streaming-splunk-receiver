package org.apache.spark.streaming.receiver

import com.splunk._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.progress.{ProgressStore, TimeProgressRecord}
import org.joda.time.{DateTime, Seconds}
import org.joda.time.format.DateTimeFormat

class SplunkReceiver(host: String,
                     port: Int,
                     username: String,
                     password: String,
                     searchQuery: String,
                     progressTracker: ProgressStore,
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

  private def getQueryWindow(differenceInSecondsFromNow: Int, requestDurationSeconds: Long, previousQueryWindowSeconds: Int) : Int = {
    val newQueryWindow: Int = differenceInSecondsFromNow match {
      case diff if diff > 3600 => 3600
      case diff if diff > 1800 => 900
      case diff if diff > 900 => 600
      case diff if diff > 300 => 300
      case _ => differenceInSecondsFromNow
    }


    if(requestDurationSeconds > previousQueryWindowSeconds) {
      //We are not catching up
      //Somehow we need to adapt
    }

    newQueryWindow
  }

  private def receive(): Unit = {

    logInfo(s"Starting Splunk receiver")

    var queryWindowSeconds = 10

    var queryStartTime: DateTime = startTime
    var queryEndTime: DateTime = startTime

    progressTracker.open()

    // Take the later value
    val progressValue = progressTracker.read()
    if(!progressValue.isEmpty) {
      val progressValueTime = DateTime.parse(progressValue, DateTimeFormat.forPattern(SPLUNK_TIME_FORMAT))
      if(progressValueTime.isAfter(startTime)) queryStartTime = progressValueTime
    }

    queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)

    logInfo(s"Initial period start: $queryStartTime, end: $queryEndTime")

    import scala.collection.JavaConversions._

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
        //TODO: Sliding window (with retries??). Depending on spark streaming settings
        exportArgs.setEarliestTime(toSplunkStringFormat(queryStartTime)) //Inclusive
        exportArgs.setLatestTime(toSplunkStringFormat(queryEndTime)) //Exclusive
        exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL)

        // Run the search with a search query and export arguments
        val exportSearch = service.export(searchQuery, exportArgs)
        // Display results using the SDK's multi-results reader for XML
        val multiResultsReader = new MultiResultsReaderXml(exportSearch)
        var eventCount = 0

        val receiveStartTime = System.nanoTime()
        for (searchResults <- multiResultsReader) {
          for (event: Event <- searchResults) {
            // Writing event by event (Unreliable streaming)
            eventCount += 1
            store(event)
          }

          //TODO: Checkpoint here?
        }
        val receiveEndTime = System.nanoTime()

        multiResultsReader.close()

        val requestDuration = (receiveEndTime - receiveStartTime) / 1000000000 //nano seconds -> seconds
        logInfo(s"Splunk request duration: $requestDuration s")

        val progressRecord = new TimeProgressRecord(queryEndTime)
        progressTracker.writeProgress(progressRecord)


        val differenceFromNow = Seconds.secondsBetween(queryEndTime, DateTime.now).getSeconds

        //bigger the difference, bigger the queryWindow can be. That way we can bring more data in. Less overhead for multiple queries.

        //TODO: If this window is too large, we could have data duplication
        //Checkpoint in the receive loop? (read max timestamp from event)
        queryWindowSeconds = getQueryWindow(differenceFromNow, requestDuration, queryWindowSeconds)
        queryStartTime = queryEndTime
        queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)
        if(queryEndTime.isAfter(DateTime.now)) queryEndTime = DateTime.now


        //DO NOT QUERY IF THERE IS NOTHING TO QUERY
        //TODO: Get some back pressure information from spark??
        if(eventCount == 0) {
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
