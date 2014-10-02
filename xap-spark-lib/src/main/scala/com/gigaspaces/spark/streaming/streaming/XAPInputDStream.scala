package com.gigaspaces.spark.streaming.streaming

import java.util.concurrent.{ExecutorService, Executors}

import com.gigaspaces.spark.streaming.streaming.utils.GigaSpaceFactory
import com.gigaspaces.streaming.XAPStream
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

/**
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
    println("Starting XAP Receiver")
    val threadPool = Executors.newFixedThreadPool(parallelReaders)
    (1 to parallelReaders).foreach(_ => threadPool.submit(new StreamReader(batchSize, readRetryInterval)))
    threadPool.shutdown()
  }

  override def onStop() = {
    // nothing to do
  }

  class StreamReader(batchSize: Int, readRetryInterval: Duration) extends Runnable {
    override def run() = {
      try {
        val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)
        val stream = new XAPStream[T](gigaSpace, template)

        println("before read")

        val items = stream.readBatch(batchSize, readRetryInterval.milliseconds)
        println("read items " + items.length)
        store(items.iterator)
      } catch {
        case e: Throwable => logError("Error reading from XAP stream", e)
      }

      run()
    }
  }

}