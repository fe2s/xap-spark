package com.gigaspaces.spark.streaming

import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}

/**
 * @author Oleksiy Dyagilev
 */
object LogHelper extends Logging {

  def setLogLevel(level:Level) {
    logInfo(s"Setting log level to [$level]")
    Logger.getRootLogger.setLevel(level)
  }

}
