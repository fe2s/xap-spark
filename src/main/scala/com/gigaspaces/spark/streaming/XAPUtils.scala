package com.gigaspaces.spark.streaming

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.openspaces.core.GigaSpace

import scala.reflect.ClassTag

/**
 * @author Oleksiy Dyagilev
 */
object XAPUtils {

  val SPACE_URL_CONF_KEY = "xap.space.url"

  def createStream[T <: java.io.Serializable : ClassTag](ssc: StreamingContext, storageLevel: StorageLevel, template: T) = {
    val spaceUrl = getSpaceUrlFromContext(ssc)
    new XAPInputDStream[T](ssc, storageLevel, spaceUrl, template)
  }

  private[streaming] def getSpaceUrlFromContext(streamingContext: StreamingContext): String = {
    getSpaceUrlFromContext(streamingContext.sparkContext)
  }

  private[streaming] def getSpaceUrlFromContext(sparkContext: SparkContext): String = {
    sparkContext.getConf.getOption(SPACE_URL_CONF_KEY).getOrElse(throw new RuntimeException(s"Config parameter $SPACE_URL_CONF_KEY is not set"))
  }


}