package com.gigaspaces.spark.streaming

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.openspaces.core.space.UrlSpaceConfigurer
import org.openspaces.core.{GigaSpaceConfigurer, GigaSpace}

import scala.util.Random

/**
 * @author Oleksiy Dyagilev
 */
object XAPWordCounter extends App {

  start()

  def start() {
    runSentenceProducer()
    runSpark()
  }

  def runSentenceProducer() {
    new Thread(new SentenceProducer).start()
  }

  def runSpark() {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val context = new StreamingContext(sparkConf, Seconds(1))

    val stream = XAPUtils.createStream[Sentence](context, StorageLevel.MEMORY_AND_DISK_SER, "jini://*/*/space", new Sentence)
    val words = stream.flatMap(_.getText.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
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