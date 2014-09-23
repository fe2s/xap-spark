package com.gigaspaces.spark.streaming

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
import org.apache.spark.streaming.dstream.DStream
import org.openspaces.core.GigaSpace

import XAPUtils._

/**
 * @author Oleksiy Dyagilev
 */
object XAPImplicits {

  implicit class XAPAwareDStream[T](dStream: DStream[T]) extends Serializable {

    def foreachRDDPartitionWithXAP(foreachFunc: (GigaSpace, Iterator[T]) => Unit): Unit = {
      dStream.foreachRDD(rdd => {
        val spaceUrl = getSpaceUrlFromContext(dStream.context)

        val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)

        val partiallyApplied = foreachFunc(gigaSpace, _:Iterator[T])
        rdd.foreachPartition(partiallyApplied)
      })
    }
  }
}

