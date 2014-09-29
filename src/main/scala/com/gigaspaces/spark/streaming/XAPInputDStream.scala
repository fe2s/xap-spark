package com.gigaspaces.spark.streaming

import java.util.concurrent.{ExecutorService, Executors}

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
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
                                                            waitTimeout: Duration,
                                                            parallelReaders: Int) extends ReceiverInputDStream[T](ssc) {

  override def getReceiver(): Receiver[T] = new XAPReceiver[T](storageLevel, spaceUrl, template, batchSize, waitTimeout, parallelReaders)

}

class XAPReceiver[T <: java.io.Serializable](storageLevel: StorageLevel,
                                             spaceUrl: String,
                                             template: T,
                                             batchSize: Int,
                                             waitTimeout: Duration,
                                             parallelReaders: Int) extends Receiver[T](storageLevel) with Logging {

  override def onStart() = {
    logInfo("Starting XAP Receiver")
    println("Starting XAP Receiver")
    val threadPool = Executors.newFixedThreadPool(parallelReaders)
    (1 to parallelReaders).foreach(_ => threadPool.submit(new StreamReader(batchSize, waitTimeout)))
    threadPool.shutdown()
  }

  override def onStop() = {
    // nothing to do
  }

  class StreamReader(batchSize: Int, waitTimeout: Duration) extends Runnable {
    override def run() = {
      try {
        val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)
        val stream = new SimpleStream[T](gigaSpace, template)

        println("before read")

        val items = stream.readBatchBlocking(batchSize, waitTimeout.milliseconds)
        println("read items " + items.size())
        store(items.iterator)
      } catch {
        case e: Throwable => logError("Error reading from XAP stream", e)
      }

      run()
    }
  }

}