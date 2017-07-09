package org.apache.spark.streaming.receiver.progress

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.apache.spark.internal.Logging


trait ProgressRecord extends Serializable {
}

class TimeProgressRecord(startTime: DateTime) extends ProgressRecord {
  override def toString: String = startTime.toString("yyyy-MM-dd'T'HH:mm:ss")
}

trait ProgressStore extends Serializable {
  def open(): Unit
  def writeProgress(progressRecord: ProgressRecord): Unit
  def read(): String
}

/**
 */
class DfsProgressStore(directory: String, name: String) extends ProgressStore with Logging
{

  if (!SparkContext.getOrCreate().isLocal) {
    require(directory.startsWith("hdfs://") || directory.startsWith("adl://"),
      "Progress writing is supported only in HDFS/ADLS when running Spark in non-local mode ")
  }

  var filePath: Path = _
  var backupFilePath: Path = _
  var progressFileSystem: FileSystem = _
  var progressBackupFileSystem: FileSystem = _

  override def open(): Unit = {
    //Check and setup file paths
    if (progressFileSystem == null) {
      filePath = new Path(s"$directory/$name/progress_file")
      progressFileSystem = filePath.getFileSystem(new Configuration())
    }

    if(progressBackupFileSystem == null) {
      backupFilePath = new Path(s"$directory/$name/progress_file.bak")
      progressBackupFileSystem = backupFilePath.getFileSystem(new Configuration())
    }
  }

  //Create file if not exists and write progress
  override def writeProgress(progressRecord: ProgressRecord): Unit = {

    var readSuccessful: Boolean = false;
    var backupSuccessful: Boolean = false;

    if(progressFileSystem.exists(filePath)) {
      //Read file value
      val stream = progressFileSystem.open(filePath)
      try{
        stream.readUTF()
        readSuccessful = true;
      } catch {
        case e: Exception =>
          logTrace(s"Failed to read progress file before writing new progress record. Path: $filePath", e)
      } finally {
        stream.close()
      }
    }

    //Create a backup of current file
    if(readSuccessful) {
      try {
        progressFileSystem.copyFromLocalFile(filePath, backupFilePath)
        backupSuccessful = true
      } catch {
        case e: Exception =>
          logError(s"Failed to create a backup of the current progress file. Path: $filePath", e)
      }
    }

    if(!readSuccessful || backupSuccessful) {
      //Write new progress/create first file
      val stream = progressFileSystem.create(filePath, true)
      try{
        stream.writeUTF(progressRecord.toString)
      }
      catch {
        case e: Exception => {
          logError(s"Failed to write new progress. Path: $filePath", e)
          throw new Exception(s"Unable to write progress to $filePath")
        }
      } finally {
        stream.close()
      }
    }
  }

  override def read(): String = {

    var readSuccessful: Boolean = false
    var fileExists: Boolean = false

    var value: String = ""

    //Try to read from primary file. If it fails, try backup

    if(progressFileSystem.exists(filePath)) {
      val stream = progressFileSystem.open(filePath)
      fileExists = true
      try {
        value = stream.readUTF()
        readSuccessful = true
      } catch {
        case e: Exception => logError(s"Unable to read progress file. Path: $filePath", e)
      } finally {
        stream.close()
      }
    }

    if(!readSuccessful) {
      //Try the backup
      if(progressBackupFileSystem.exists(backupFilePath)) {
        fileExists = true
        val stream = progressBackupFileSystem.open(backupFilePath)
        try {
          value = stream.readUTF()
          readSuccessful = true
        } catch {
          case e: Exception => logError(s"Unable to read backup progress file. Path: $backupFilePath", e)
        } finally {
          stream.close()
        }
      }
    }

    if(fileExists && !readSuccessful) {
      throw new Exception(s"Unable to read progress file for $name")
    }

    value
  }
}