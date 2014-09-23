package com.gigaspaces.spark.streaming

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.openspaces.core.space.UrlSpaceConfigurer
import org.openspaces.core.{GigaSpaceConfigurer, GigaSpace}
import org.apache.log4j.{Level, Logger}


import scala.util.Random

/**
 * @author Oleksiy Dyagilev
 */
object XAPWordCounter extends App {

  start()

  def start() {
    LogHelper.setLogLevel(Level.WARN)

    runSentenceProducer()
    runSpark()
  }

  def runSentenceProducer() {
    new Thread(new SentenceProducer).start()
  }

  def runSpark() {
    val sparkConf = new SparkConf().setAppName("XAPWordCount").setMaster("local[*]")
    val context = new StreamingContext(sparkConf, Seconds(1))
    context.checkpoint(".")

    // create XAP stream
    val spaceUrl = "jini://*/*/space"
    val stream = XAPUtils.createStream[Sentence](context, StorageLevel.MEMORY_AND_DISK_SER, spaceUrl, new Sentence)
    val words = stream.flatMap(_.getText.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // Update the cumulative count using updateStateByKey
    // This will give a DStream made of state (which is the cumulative count of the words)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val stateDStream = wordDstream.updateStateByKey[Int](updateFunc)

    // output counts to XAP
    stateDStream.foreachRDD((rdd: RDD[(String, Int)]) => {
      rdd.foreachPartition(partitionRecords => {
        val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)
        val wordCounts = partitionRecords.map {
          case (word, count) => new WordCount(word, count)
        }
        gigaSpace.writeMultiple(wordCounts.toArray)
      })
    })

    stateDStream.print()
    context.start()
    context.awaitTermination()
  }

}

class SentenceProducer extends Runnable {

  val random = new Random()
  val gigaSpace = GigaSpaceFactory.getOrCreate("jini://*/*/space")
  val sentences = Seq("the cow jumped over the moon",
    "an apple a day keeps the doctor away",
    "four score and seven years ago",
    "snow white and the seven dwarfs",
    "i am at two with nature")

  override def run() {
    val rndSentenceId = random.nextInt(sentences.length - 1)
    val rndSentence = new Sentence(sentences(rndSentenceId))
    gigaSpace.write(rndSentence)
    Thread.sleep(500)
    run()
  }
}