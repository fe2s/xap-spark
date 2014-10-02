package com.gigaspaces.spark.streaming.streaming.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
 * @author Oleksiy Dyagilev
 */
object LogHelper extends Logging {

  def setLogLevel(level:Level) {
    logInfo(s"Setting log level to [$level]")
    Logger.getRootLogger.setLevel(level)
  }

}
