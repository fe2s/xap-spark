package com.gigaspaces.spark.streaming.streaming.utils

import com.gigaspaces.spark.streaming.streaming.XAPInputDStream
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.{Milliseconds, Duration, Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
 * @author Oleksiy Dyagilev
 */
object XAPUtils {

  val SPACE_URL_CONF_KEY = "xap.space.url"

  def createStream[T <: java.io.Serializable : ClassTag](ssc: StreamingContext, template: T, batchSize:Int, readRetryInterval: Duration = Milliseconds(100), parallelReaders: Int, storageLevel: StorageLevel = MEMORY_AND_DISK_SER) = {
    val spaceUrl = getSpaceUrlFromContext(ssc)
    new XAPInputDStream[T](ssc, storageLevel, spaceUrl, template, batchSize, readRetryInterval, parallelReaders)
  }

  private[streaming] def getSpaceUrlFromContext(streamingContext: StreamingContext): String = {
    getSpaceUrlFromContext(streamingContext.sparkContext)
  }

  private[streaming] def getSpaceUrlFromContext(sparkContext: SparkContext): String = {
    sparkContext.getConf.getOption(SPACE_URL_CONF_KEY).getOrElse(throw new RuntimeException(s"Config parameter $SPACE_URL_CONF_KEY is not set"))
  }

}
