package com.gigaspaces.spark.streaming.wordcounter

import org.openspaces.core.GigaSpaceConfigurer
import org.openspaces.core.space.UrlSpaceConfigurer

import scala.util.Random

/**
 * @author Oleksiy Dyagilev
 */
object Feeder extends App {

  val gigaSpace = {
    val urlSpaceConfigurer = new UrlSpaceConfigurer("jini://*/*/space")
    new GigaSpaceConfigurer(urlSpaceConfigurer.space()).gigaSpace()
  }

  start()

  def start() {
    println("Starting sentence producer")
    runSentenceProducer()
  }

  def runSentenceProducer() {
    new Thread(new SentenceProducer).start()
  }

  class SentenceProducer extends Runnable {

    val random = new Random()
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

}

