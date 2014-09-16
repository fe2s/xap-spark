package com.gigaspaces.spark.streaming

import java.util.concurrent.{ExecutorService, Executors}

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

/**
 * @author Oleksiy Dyagilev
 */
class XAPInputDStream[T <: java.io.Serializable : ClassTag](@transient ssc: StreamingContext,
                                                            storageLevel: StorageLevel,
                                                            spaceUrl: String,
                                                            template: T) extends ReceiverInputDStream[T](ssc) {

  override def getReceiver(): Receiver[T] = new XAPReceiver[T](storageLevel, spaceUrl, template)

}

class XAPReceiver[T <: java.io.Serializable](storageLevel: StorageLevel,
                                             spaceUrl: String,
                                             template: T) extends Receiver[T](storageLevel) with Logging {

  override def onStart() = {
    // TODO: make configurable
    val threadsNumber = 1
    val threadPool = Executors.newFixedThreadPool(threadsNumber)
    (1 to threadsNumber).foreach(_ => threadPool.submit(new StreamReader))
    threadPool.shutdown()
  }

  override def onStop() = {
    // nothing to do
  }

  class StreamReader extends Runnable {
    override def run() = {
      try {
        val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)
        val stream = new SimpleStream[T](gigaSpace, template)

        // TODO: make config.
        val items = stream.readBatch(2)
        store(items.iterator)
      } catch {
        case e: Throwable => logError("Error reading from XAP stream", e)
      }

      // TODO:
      Thread.sleep(1000)
      run()
    }
  }

}