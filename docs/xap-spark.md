# Introduction #

Real-time processing is becoming more and more popular. [Spark Streaming](https://spark.apache.org/streaming/) is an extension of the core Spark API that allows scalable, high-throughput, fault-tolerant stream processing of live data streams.

Spark Streaming has many use cases: user activity analytics on web, recommendation systems, censor data analytics, fraud detection, sentiment analytics and many more.

Data can be ingested to Spark cluster from many sources like HDS, Kafka, Flume, etc and be processed using complex algorithms expressed with high-level functions like `map`, `reduce`, `join` and `window`. Finally, processed data can be pushed out to filesystems or databases.

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/spark-streaming.jpg)

# Challenge #

Spark cluster keeps intermediate chunks of data (RDD) in memory and if required rarely touches HDFS to checkpoint stateful computation, therefore it is able to process huge volumes of data at in-memory speed. However, in many cases the overall performance is limited by slow input and output data sources that are not able to stream and store data with in-memory speed.

In this pattern we address performance challenge by integrating Spark Streaming with XAP. XAP is used as a stream data source and scalable, fast, reliable persistent storage.  

As part of this integration pattern we demonstrate how to build application that consumes live stream of text and displays top 10 five-letter words over a sliding window in real-time.

# Solution #

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/high-level.jpg)

1.	Producer writes data to XAP stream
2.	Spark worker reads data from XAP stream and propagates it further for computation
3.	Spark saves computation results to XAP datagrid where they can be queried to display on UI

Let’s discuss this in more details.

## XAP Stream ##

On XAP side we introduce conception of stream. Please find `XAPStream` – a stream implementation that supports writing data in single and batch modes and reading in batch mode. `XAPStream` leverages XAP’s `FIFO`(First In, First Out) capabilities.

Here is an example how one can write data to `XAPStream`. Let’s consider we are building a Word Counter application and would like to write sentences of text to the stream.

At first we create a data model that represents a sentence. Note, that space class should be annotated with `FIFO` support. 

```java
@SpaceClass(fifoSupport = FifoSupport.OPERATION)
public class Sentence implements Serializable {
    private String id;
    private String text;
    public Sentence() {
    }
[getters setters omitted for brevity]
}
```
> Complete sources of Sentence.java can be found [here](https://github.com/fe2s/xap-spark/blob/master/word-counter-demo/feeder/src/main/scala/com/gigaspaces/spark/streaming/wordcounter/Feeder.scala)

## Spark Input DStream ##

In order to ingest data from XAP to Spark, we implemented custom `ReceiverInputDStream` that starts `XAPReceiver` on Spark worker nodes to receive data. 

`XAPReceiver` is a stream consumer that reads batches of data in multiple threads in parallel to achieve the maximum throughput.

`XAPInputDStream` can be created using the following function in `XAPUtils` object.

```scala
/**
   * Creates InputDStream with GigaSpaces XAP used as external data store
   * 
   * @param ssc streaming context
   * @param storageLevel RDD persistence level
   * @param template template used to match items when reading from XAP stream
   * @param batchSize number of items to read from 
   * @param readRetryInterval time to wait till the next read attempt if nothing consumed
   * @param parallelReaders number of parallel readers
   * @tparam T Class type of the object of this stream 
   * @return Input DStream
   */
  def createStream[T <: java.io.Serializable : ClassTag](ssc: StreamingContext, template: T, batchSize:Int, readRetryInterval: Duration = Milliseconds(100), parallelReaders: Int, storageLevel: StorageLevel = MEMORY_AND_DISK_SER){…} 
```

Here is an example of creating XAP Input stream. At first we set XAP space url to Spark config:

```scala
  val sparkConf = new SparkConf()
      .setAppName("XAPWordCount")
      .setMaster(config.sparkMaster)
      .setJars(Seq(config.jarLocation))
      .set(XAPUtils.SPACE_URL_CONF_KEY, "jini://*/*/space")
```

And then we create a stream by merging two parallel sub-streams:

```scala
val numStreams = 2
val streams = (1 to numStreams).map(_ => XAPUtils.createStream[Sentence](context, new Sentence(), 50, Milliseconds(100), 4))
val stream = context.union(streams)
```

Once stream created, once can apply any Spark functions like `map`, `filter`, `reduce`, `transform`, etc.

For instance to compute word counter of five-letter words over a sliding window, one can do the following

```scala
val words = stream.flatMap(_.getText.split(" ")).filter(_.size == 5)
val wordCountWindow = words
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(5))
      .map { case (word, count) => (count, word)}
      .transform(_.sortByKey(ascending = false))
```

Output Spark computation results to XAP

Output operations allow `DStream`’s data to be pushed out external systems. Please refer to [Spark documentation](https://spark.apache.org/docs/1.1.0/streaming-programming-guide.html#output-operations-on-dstreams) for details 

To minimize the cost of creating XAP connection for each `RDD`, we created a connection pool named `GigaSpaceFactory`. Here is an example how to output `RDD` to XAP.

```scala
wordCountWindow.foreachRDD(rdd => {
      val gigaSpace = GigaSpaceFactory.getOrCreate(config.spaceUrl)
      val topList = rdd.take(10).map { case (count, word) => new WordCount(word, count)}
      gigaSpace.write(new TopWordCounts(topList))
    })
```

Please note that in this example XAP connection created and data written from Spark driver. In some cases one may want to write data from Spark worker. Please refer to Spark documentation, it explains different design patterns using `foreachRDD`.

# Word Counter Demo #
 
As part of this integration pattern we demonstrate how to build application that consumes live stream of text and displays top 10 five-letter words over a sliding window in real-time.

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/spark-word-counter.jpg)

## High-level design ##

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/high-level.jpg)

Feeder is a standalone java application that reads book from text file and writes lines to XAP Stream. 

Stream is consumed by Spark cluster which performs all necessary computing and stores results in XAP space. End user is browsing web page hosted in Web PU that continuously updates dashboard with AJAX requests backed by rest service

## Installing and building the Demo application ##

