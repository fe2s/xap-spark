package com.gigaspaces.spark.streaming.wordcounter

import com.gigaspaces.spark.streaming.streaming.utils.XAPUtils._
import com.gigaspaces.spark.streaming.streaming.utils.{GigaSpaceFactory, LogHelper, XAPUtils}
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import scopt.OptionParser

/**
 * @author Oleksiy Dyagilev
 */
case class Config(spaceUrl:String, sparkMaster:String, jarLocation: String)

object XAPWordCounter extends App {

  val TOP_K = 10

  start()

  def start() = parseArgs map runSpark

  def parseArgs(): Option[Config] = {
    val parser = new OptionParser[Config]("com.gigaspaces.spark.streaming.wordcounter.XAPWordCounter") {
      opt[String]('s', "space-url") action {
        (x, c) => c.copy(spaceUrl = x)
      }
      opt[String]('m', "spark-master") action {
        (x, c) => c.copy(sparkMaster = x)
      }
      opt[String]('j', "jar-location") action {
        (x, c) => c.copy(jarLocation = x)
      }
    }

    val defaultConfig = Config("jini://*/*/space", "local[*]", "")
    parser.parse(args.toSeq, defaultConfig)
  }

  def runSpark(config:Config){
    LogHelper.setLogLevel(Level.WARN)

    println(s"Starting Spark with $config")

    val sparkConf = new SparkConf()
      .setAppName("XAPWordCount")
      .setMaster(config.sparkMaster)
      .setJars(Seq(config.jarLocation))
      .set(SPACE_URL_CONF_KEY, config.spaceUrl)

    val context = new StreamingContext(sparkConf, Seconds(1))
    context.checkpoint("./checkpoint")

    // create XAP stream by merging parallel sub-streams
    val numStreams = 2
    val streams = (1 to numStreams).map(_ => XAPUtils.createStream(context, new Sentence(), 50, Milliseconds(100), 4))
    val stream = context.union(streams)

    // computation
    val words = stream.flatMap(_.getText.split(" ")).filter(_.size == 5)
    val wordCountWindow = words
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(5))
      .map { case (word, count) => (count, word)}
      .transform(_.sortByKey(ascending = false))
//      .mapPartitions(_.take(TOP_K))

    // output to XAP
    wordCountWindow.foreachRDD(rdd => {
      val gigaSpace = GigaSpaceFactory.getOrCreate(config.spaceUrl)
      val topList = rdd.take(TOP_K).map { case (count, word) => new WordCount(word, count)}
      topList.foreach(println)
      println("-----")
      gigaSpace.write(new TopWordCounts(topList))
    })

    context.start()
    context.awaitTermination()
  }

}