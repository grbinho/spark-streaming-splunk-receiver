package io.grba.spark

import com.splunk._

object SplunkExport {
  def main(args: Array[String]) {

    val loginArgs: ServiceArgs = new ServiceArgs
    loginArgs.setUsername("export")
    loginArgs.setPassword("export")
    loginArgs.setHost("localhost")
    loginArgs.setPort(8089)
    loginArgs.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2)

    val service = Service.connect(loginArgs)

    // Export search
    // Create an argument map for the export arguments// Create an argument map for the export arguments

    val exportArgs = new JobExportArgs
    //TODO: Sliding window (with retries??). Depending on spark streaming settings
    //exportArgs.setEarliestTime("-1h")
    //exportArgs.setLatestTime("now")
    exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL)

    // Run the search with a search query and export arguments
    val mySearch = "search index=nytaxi | fields vendor_name, Trip_Pickup_DateTime | fields - _* | head 100"
    val exportSearch = service.export(mySearch, exportArgs)

    // Display results using the SDK's multi-results reader for XML
    val multiResultsReader = new MultiResultsReaderXml(exportSearch)

    var counter = 0 // count the number of events
    import scala.collection.JavaConversions._

    for (searchResults <- multiResultsReader) {
      for (event <- searchResults) {
        System.out.println("***** Event " + {
          counter += 1; counter - 1
        } + " *****")

        for (key <- event.keySet) {
          System.out.println("   " + key + ":  " + event.get(key))
        }
      }
    }



    multiResultsReader.close()
  }
}
