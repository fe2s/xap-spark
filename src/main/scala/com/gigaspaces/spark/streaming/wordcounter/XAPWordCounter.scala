package com.gigaspaces.spark.streaming.wordcounter

import com.gigaspaces.spark.streaming.utils.{XAPUtils, LogHelper, GigaSpaceFactory}
import XAPUtils._
import com.gigaspaces.spark.streaming.utils.{LogHelper, GigaSpaceFactory}
import com.gigaspaces.spark.streaming.{Sentence, WordCount}
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    val sparkConf = new SparkConf()
      .setAppName("XAPWordCount")
            .setMaster("local[*]")
//      .setMaster("spark://fe2s:7077")
      .set(SPACE_URL_CONF_KEY, "jini://*/*/space")

    val context = new StreamingContext(sparkConf, Seconds(1))
    context.checkpoint(".")

    // create XAP stream
    val numStreams = 1
    val streams = (1 to numStreams).map(_ => XAPUtils.createStream[Sentence](context, new Sentence, 50, Seconds(3), 4))
    val stream = context.union(streams)
    val words = stream.flatMap(_.getText.split(" "))
    val wordDStream = words.map(x => (x, 1))

    // update the cumulative count using updateStateByKey
    // this will give a DStream made of state (which is the cumulative count of the words)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val wordCountStream = wordDStream.updateStateByKey[Int](updateFunc)

    // output counts to XAP
    wordCountStream.foreachRDD((rdd: RDD[(String, Int)]) => {
      rdd.foreachPartition(partitionRecords => {
        val spaceUrl = "jini://*/*/space"
        val gigaSpace = GigaSpaceFactory.getOrCreate(spaceUrl)
        val wordCounts = partitionRecords.map {
          case (word, count) => new WordCount(word, count)
        }
        if (wordCounts.nonEmpty) {
          gigaSpace.writeMultiple(wordCounts.toArray)
        }
      })
    })

    wordCountStream.print()
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