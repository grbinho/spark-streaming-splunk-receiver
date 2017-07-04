package io.grba.spark.streaming.receiver

import com.splunk._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.joda.time.DateTime

class SplunkReceiver(host: String,
                     port: Int,
                     username: String,
                     password: String,
                     searchQuery: String,
                     startTime: DateTime,
                     queryWindowSeconds: Int)
  extends Receiver[Event](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart(): Unit = {
    new Thread("Splunk Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop(): Unit = {

  }

  /* Accepted splunk date time format
   */
  private def toSplunkStringFormat(value: DateTime) = value.toString("yyyy-MM-dd'T'HH:mm:ss")

  private def receive(): Unit = {

    //TODO: In case of a restart, we should read start time from somewhere...
    var queryStartTime = startTime
    var queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)

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
        var counter = 0 // count the number of events

        for (searchResults <- multiResultsReader) {
          for (event: Event <- searchResults) {
            // Writing event by event (Unreliable streaming)
            store(event)
          }
        }

        multiResultsReader.close()

        println("Wait a bit")

        Thread.sleep(10000)

        println("Done waiting")

        //Move query window
        queryStartTime = queryEndTime
        queryEndTime = queryStartTime.plusSeconds(queryWindowSeconds)
      }

      //TODO: Close other resources and retry
      //TODO: Store time window state?
      restart("Trying to connect again")
    } catch {
      case e: Throwable => restart("Error receiving data from Splunk", e)
    }
  }
}
