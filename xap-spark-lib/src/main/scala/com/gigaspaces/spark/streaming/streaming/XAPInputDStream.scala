package com.gigaspaces.spark.streaming.streaming

import java.util.concurrent.{Executors}

import com.gigaspaces.spark.streaming.streaming.utils.GigaSpaceFactory
import com.gigaspaces.streaming.XAPStream
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

/**
 * InputDStream with GigaSpaces XAP used as external data store.
 *
 *
 * @param ssc streaming context
 * @param storageLevel RDD persistence level
 * @param spaceUrl space url
 * @param template template used to match items when reading from XAP stream
 * @param batchSize number of items to read from
 * @param readRetryInterval time to wait till the next read attempt if nothing consumed
 * @param parallelReaders number of parallel readers
 * @tparam T Class type of the object of this stream
 *
 * @author Oleksiy Dyagilev
 */
class XAPInputDStream[T <: java.io.Serializable : ClassTag](@transient ssc: StreamingContext,
                                                            storageLevel: StorageLevel,
                                                            spaceUrl: String,
                                                            template: T,
                                                            batchSize: Int,
                                                            readRetryInterval: Duration,
                                                            parallelReaders: Int) extends ReceiverInputDStream[T](ssc) {

  override def getReceiver(): Receiver[T] = new XAPReceiver[T](storageLevel, spaceUrl, template, batchSize, readRetryInterval, parallelReaders)

}

class XAPReceiver[T <: java.io.Serializable](storageLevel: StorageLevel,
                                             spaceUrl: String,
                                             template: T,
                                             batchSize: Int,
                                             readRetryInterval: Duration,
                                             parallelReaders: Int) extends Receiver[T](storageLevel) with Logging {

  override def onStart() = {
    logInfo("Starting XAP Receiver")
    val threadPool = Executors.newFixedThreadPool(parallelReaders)
    (1 to parallelReaders).foreach(_ => threadPool.submit(new StreamReader(batchSize, readRetryInterval)))
    threadPool.shutdown()
  }

  override def onStop() = {
    // nothing to do
  }

  class StreamReader(batchSize: Int, readRetryInterval: Duration) extends Runnable {
    override def run() = {
      while (!isStopped()) {
        try {
          val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)
          val stream = new XAPStream[T](gigaSpace, template)

          val items = stream.readBatch(batchSize, readRetryInterval.milliseconds)
          logDebug("read items " + items.length)
          store(items.iterator)
        } catch {
          case e: Throwable => logError("Error reading from XAP stream", e)
        }
      }
    }
  }

}