package com.gigaspaces.spark.streaming.streaming.utils

import org.openspaces.core.{GigaSpaceConfigurer, GigaSpace}
import org.openspaces.core.space.UrlSpaceConfigurer
import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.decorateAsScala._


/**
 * Ensures single GigaSpace instance per JVM (Spark worker)
 *
 * @author Oleksiy Dyagilev
 */
object GigaSpaceFactory {

  private val gigaSpaces = new ConcurrentHashMap[String, GigaSpace]().asScala

  // TODO: add group, locator
  def getOrCreate(spaceUrl: String): GigaSpace = {
    gigaSpaces.getOrElse(spaceUrl, createInstanceIfRequired(spaceUrl))
  }

  private def createInstanceIfRequired(spaceUrl: String): GigaSpace = {
    this.synchronized {
      gigaSpaces.get(spaceUrl) match {
        case Some(gs) => gs
        case None =>
          val urlSpaceConfigurer = new UrlSpaceConfigurer(spaceUrl)
          val gs = new GigaSpaceConfigurer(urlSpaceConfigurer.space()).gigaSpace()
          gigaSpaces.put(spaceUrl, gs)
          gs
      }
    }
  }

}
