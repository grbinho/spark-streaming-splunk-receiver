package org.apache.spark.streaming.receiver

import com.splunk._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.progress.{ProgressStore, TimeProgressRecord}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class SplunkReceiver(host: String,
                     port: Int,
                     username: String,
                     password: String,
                     searchQuery: String,
                     queryWindowSeconds: Int,
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

  /* Accepted splunk date time format
   */
  private def toSplunkStringFormat(value: DateTime) = value.toString(SPLUNK_TIME_FORMAT)

  private def receive(): Unit = {

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
        for (searchResults <- multiResultsReader) {
          for (event: Event <- searchResults) {
            // Writing event by event (Unreliable streaming)
            store(event)
          }
        }

        val progressRecord = new TimeProgressRecord(queryEndTime)
        progressTracker.writeProgress(progressRecord)

        //TODO: Correlate with now. We should not go to the future.
        //Untill now bigger time span, and when we catch up, queryEndTime should be now

        queryStartTime = queryEndTime
        queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)

        multiResultsReader.close()

        //TODO: Decide how much to sleep depending on how many records are coming in.

        Thread.sleep(1000)
      }

      restart("Trying to connect again")
    } catch {
      case e: Throwable => restart("Error receiving data from Splunk", e)
    }
  }
}
