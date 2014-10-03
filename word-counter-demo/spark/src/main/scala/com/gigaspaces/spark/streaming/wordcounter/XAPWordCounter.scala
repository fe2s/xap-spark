package com.gigaspaces.spark.streaming.wordcounter

import com.gigaspaces.spark.streaming.streaming.utils.XAPUtils._
import com.gigaspaces.spark.streaming.streaming.utils.{GigaSpaceFactory, LogHelper, XAPUtils}
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import scala.util.Random

/**
 * @author Oleksiy Dyagilev
 */
object XAPWordCounter extends App {

  start()

  def start() {
    LogHelper.setLogLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("XAPWordCount")
            .setMaster("local[*]")
//      .setMaster("spark://fe2s:7077")
      .set(SPACE_URL_CONF_KEY, "jini://*/*/space")

    val context = new StreamingContext(sparkConf, Seconds(1))
    context.checkpoint(".")

    // create XAP stream
    val numStreams = 1
    val streams = (1 to numStreams).map(_ => XAPUtils.createStream[Sentence](context, new Sentence, 50, Milliseconds(100), 4))
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