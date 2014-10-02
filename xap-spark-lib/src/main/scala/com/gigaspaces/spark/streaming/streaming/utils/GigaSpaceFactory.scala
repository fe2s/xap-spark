package com.gigaspaces.spark.streaming.streaming.utils

import org.openspaces.core.{GigaSpaceConfigurer, GigaSpace}
import org.openspaces.core.space.UrlSpaceConfigurer
import scala.collection.mutable


/**
 * Ensures single GigaSpace instance per JVM (Spark worker)
 *
 * @author Oleksiy Dyagilev
 */
object GigaSpaceFactory {

  private val gigaSpaces = mutable.Map[String, GigaSpace]()

  def getOrCreate(spaceUrl: String): GigaSpace = {
    def createInstance = {
      val urlSpaceConfigurer = new UrlSpaceConfigurer(spaceUrl)
      new GigaSpaceConfigurer(urlSpaceConfigurer.space()).gigaSpace()
    }

    gigaSpaces.getOrElseUpdate(spaceUrl, createInstance)
  }

}
