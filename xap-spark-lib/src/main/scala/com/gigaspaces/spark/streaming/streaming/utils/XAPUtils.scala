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

  /**
   * Creates InputDStream with GigaSpaces XAP used as external data store
   *
   * @param ssc streaming context
   * @param storageLevel RDD persistence level
   * @param template template used to match items when reading from XAP stream
   * @param batchSize number of items to read from
   * @param readRetryInterval time to wait till the next read attempt if nothing consumed
   * @param parallelReaders number of parallel readers
   * @tparam T Class type of the object of this stream
   * @return Input DStream
   */
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
