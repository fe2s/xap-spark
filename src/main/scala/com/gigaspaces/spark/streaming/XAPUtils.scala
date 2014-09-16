package com.gigaspaces.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
 * @author Oleksiy Dyagilev
 */
object XAPUtils {

  def createStream[T <: java.io.Serializable : ClassTag](ssc: StreamingContext, storageLevel: StorageLevel, spaceUrl: String, template: T) = {
    new XAPInputDStream[T](ssc, storageLevel, spaceUrl, template)
  }

}
