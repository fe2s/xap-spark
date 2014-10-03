package com.gigaspaces.spark.streaming.wordcounter

import java.util.concurrent.Executors

import org.openspaces.core.{GigaSpace, GigaSpaceConfigurer}
import org.openspaces.core.space.UrlSpaceConfigurer
import scopt.OptionParser

import scala.io.Source

/**
 * @author Oleksiy Dyagilev
 */

case class Config(space: String, group: Option[String], sentenceNumPerSecond: Int, threadsNum: Int)

object Feeder extends App {

  start()

  def start() {
    println("Starting sentence producer")

    val parser = new OptionParser[Config]("com.gigaspaces.spark.streaming.wordcounter.Feeder") {
      opt[String]('s', "space") action {
        (x, c) => c.copy(space = x)
      }
      opt[String]('g', "group") action {
        (x, c) => c.copy(group = Some(x))
      }
      opt[Int]('n', "sentence-num-per-second") action {
        (x, c) => c.copy(sentenceNumPerSecond = x)
      }
      opt[Int]('t', "threads-num") action {
        (x, c) => c.copy(threadsNum = x)
      }
    }

    val defaultConfig = Config("space", None, 100, 1)

    parser.parse(args.toSeq, defaultConfig) map { conf =>
      println(conf + "\n")
      runSentenceProducers(conf)
    }

  }

  def runSentenceProducers(conf: Config) {
    val gigaSpace = {
      val configurer = new UrlSpaceConfigurer(s"jini://*/*/${conf.space}")
      val withGroup = conf.group.map(configurer.lookupGroups).getOrElse(configurer)
      new GigaSpaceConfigurer(withGroup.space()).gigaSpace()
    }

    val threadPool = Executors.newFixedThreadPool(conf.threadsNum)
    (1 to conf.threadsNum).foreach(_ => threadPool.submit(new SentenceProducer(gigaSpace, conf.sentenceNumPerSecond)))
    threadPool.shutdown()
  }

  class SentenceProducer(gigaSpace: GigaSpace, sentenceNumPerSecond: Int) extends Runnable {
    val file = "/mobydick.txt"
    //    val file = "/test.txt"

    val lines = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().toList
    val stream = Stream.continually(lines).flatten

    override def run() {
      while (true) {
        val startTime = System.currentTimeMillis()
        val sentences = stream.take(sentenceNumPerSecond).map(new Sentence(_)).toArray

        println(s"sending ${sentences.length} sentences")
        gigaSpace.writeMultiple(sentences)

        val timeTaken = System.currentTimeMillis() - startTime
        if (timeTaken < 1000) {
          Thread.sleep(1000 - timeTaken)
        } else {
          println(s"WARN: Sending $sentenceNumPerSecond took $timeTaken")
        }
      }
    }
  }

}

