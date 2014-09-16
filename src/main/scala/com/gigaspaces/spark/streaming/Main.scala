package com.gigaspaces.spark.streaming

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.openspaces.core.space.UrlSpaceConfigurer
import org.openspaces.core.{GigaSpaceConfigurer, GigaSpace}

/**
 * @author Oleksiy Dyagilev
 */
object Main extends App {

  run()

  def run() {

//    (1 to 1).foreach(_ => println("============"))

//    val p = new Person()
//    p.test()
//
//    println("Hello")
//
//    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val context = new StreamingContext(sparkConf, Seconds(1))

    val stream = XAPUtils.createStream[Person](context, StorageLevel.MEMORY_AND_DISK_SER, "jini://*/*/space", new Person)

    val names = stream.map(p => p.getName)
    names.print()

//    val host = "localhost"
//    val port = 9999
//
//    val lines = context.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
//
//    //    context.file
//
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts.print()
    context.start()
    context.awaitTermination()

//    val gigaSpace = GigaSpaceFactory.getInstance("jini://*/*/space")
//    val person = new Person
//    person.setName("ddddd")
//
//    gigaSpace.write(person)

  }

}
