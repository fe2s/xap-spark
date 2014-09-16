package com.gigaspaces.spark.streaming

import com.gigaspaces.spark.streaming.utils.GigaSpaceFactory

/**
 * @author Oleksiy Dyagilev
 */
object GenerateData extends App {

  def run() = {
        val gigaSpace = GigaSpaceFactory.getOrCreate("jini://*/*/space")
        for (i <- 1 to 20) {
          val person = new Person
          person.setName("name " + i)
          gigaSpace.write(person)
        }
  }

  run()

}
