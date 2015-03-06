**Table of Contents**

- [Introduction](#introduction)
- [Challenge](#challenge)
- [Solution](#solution)
	- [XAP Stream](#xap-stream)
	- [Spark Input DStream](#spark-input-dstream)
	- [Output Spark computation results to XAP](#output-spark-computation-results-to-xap)
- [Word Counter Demo](#word-counter-demo)
	- [High-level design](#high-level-design)
	- [Installing and building the Demo application](#installing-and-building-the-demo-application)
	- [Deploying XAP Space and Web PU](#deploying-xap-space-and-web-pu)
	- [Launch Spark Application](#launch-spark-application)
		- [Option A. Run embedded Spark cluster](#option-a.-run-embedded-spark-cluster)
		- [Option B. Run Spark standalone mode cluster](#option-b.-run-spark-standalone-mode-cluster)
			- [Run Spark](#run-spark)
			- [Submit application](#submit-application)
	- [Launch Feeder application](#launch-feeder-application)

# <a name="introduction"/>Introduction #

Real-time processing is becoming more and more popular. [Spark Streaming](https://spark.apache.org/streaming/) is an extension of the core Spark API that allows scalable, high-throughput, fault-tolerant stream processing of live data streams.

Spark Streaming has many use cases: user activity analytics on web, recommendation systems, censor data analytics, fraud detection, sentiment analytics and more.

Data can be ingested to Spark cluster from many sources like HDS, Kafka, Flume, etc and can be processed using complex algorithms expressed with high-level functions like `map`, `reduce`, `join` and `window`. Finally, processed data can be pushed out to filesystems or databases.

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/spark-streaming.jpg)

# <a name="challenge"/>Challenge #

Spark cluster keeps intermediate chunks of data (RDD) in memory and, if required, rarely touches HDFS to checkpoint stateful computation, therefore it is able to process huge volumes of data at in-memory speed. However, in many cases the overall performance is limited by slow input and output data sources that are not able to stream and store data with in-memory speed.

# <a name="solution"/>Solution #

In this pattern we address performance challenge by integrating Spark Streaming with XAP. XAP is used as a stream data source and a scalable, fast, reliable persistent storage.

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/high-level.jpg)

1.	Producer writes the data to XAP stream
2.	Spark worker reads the data from XAP stream and propagates it further for computation
3.	Spark saves computation results to XAP datagrid where they can be queried to display on UI

Let’s discuss this in more details.

## <a name="xap-stream"/>XAP Stream ##

On XAP side we introduce the concept of stream. Please find `XAPStream` – an implementation that supports writing data in single and batch modes and reading in batch mode. `XAPStream` leverages XAP’s `FIFO` (First In, First Out) capabilities.

Here is an example how one can write data to `XAPStream`. Let’s consider we are building a Word Counter application and would like to write sentences of text to the stream.

At first we create a data model that represents a sentence. Note, that the space class should be annotated with `FIFO` support.

```java
@SpaceClass(fifoSupport = FifoSupport.OPERATION)
public class Sentence implements Serializable {
    private String id;
    private String text;
    [getters setters omitted for brevity]
}
```
> Complete sources of Sentence.java can be found [here](https://github.com/fe2s/xap-spark/blob/master/word-counter-demo/space-model/src/main/java/com/gigaspaces/spark/streaming/wordcounter/Sentence.java)

## <a name="spark-input-dstream"/>Spark Input DStream ##

In order to ingest data from XAP to Spark, we implemented a custom `ReceiverInputDStream` that starts the `XAPReceiver` on Spark worker nodes to receive the data.

`XAPReceiver` is a stream consumer that reads batches of data in multiple threads in parallel to achieve the maximum throughput.

`XAPInputDStream` can be created using the following function in `XAPUtils` object.

```scala
/**
   * Creates InputDStream with GigaSpaces XAP used as an external data store
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

Here is an example of creating XAP Input stream. At first we set XAP space url in Spark config:

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
val streams = (1 to numStreams).map(_ => XAPUtils.createStream(context, new Sentence(), 50, Milliseconds(100), 4))
val stream = context.union(streams)
```

Once the stream is created, we can apply any Spark functions like `map`, `filter`, `reduce`, `transform`, etc.

For instance, to compute a word counter of five-letter words over a sliding window, one can do the following:

```scala
val words = stream.flatMap(_.getText.split(" ")).filter(_.size == 5)
val wordCountWindow = words
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(5))
      .map { case (word, count) => (count, word)}
      .transform(_.sortByKey(ascending = false))
```

## <a name="output-spark-computation-results-to-xap"/>Output Spark computation results to XAP ##

Output operations allow the `DStream`’s data to be pushed out to external systems. Please refer to [Spark documentation](https://spark.apache.org/docs/1.1.0/streaming-programming-guide.html#output-operations-on-dstreams) for the details.

To minimize the cost of creating XAP connection for each `RDD`, we created a connection pool named `GigaSpaceFactory`. Here is an example how to output `RDD` to XAP:

```scala
wordCountWindow.foreachRDD(rdd => {
      val gigaSpace = GigaSpaceFactory.getOrCreate(config.spaceUrl)
      val topList = rdd.take(10).map { case (count, word) => new WordCount(word, count)}
      gigaSpace.write(new TopWordCounts(topList))
})
```

> Please, note that in this example a XAP connection is created and data is written from Spark driver. In some cases, one may want to write data from the Spark worker. Please, refer to Spark documentation - it explains different design patterns using `foreachRDD`.

# <a name="word-counter-demo"/>Word Counter Demo #

As a part of this integration pattern, we demonstrate how to build an application that consumes live stream of text and displays top 10 five-letter words over a sliding window in real-time. The user interface consists of a simple single page web application displaying a table of top 10 words and a word cloud. The data on UI is updated every second.

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/spark-word-counter.jpg)

## <a name="high-level-design"/>High-level design ##

The high-level design diagram of the Word Counter Demo is below:

![alt tag](https://github.com/fe2s/xap-spark/blob/master/docs/images/example.jpg)

1. Feeder is a standalone scala application that reads book from text file in a cycle and writes lines to XAP Stream.
2. Stream is consumed by the Spark cluster which performs all necessary computing.
3. Computation results are stored in the XAP space.
4. End user is browsing the web page hosted in a Web PU that continuously updates dashboard with AJAX requests backed by the rest service.

## <a name="installing-and-building-the-demo-application"/>Installing and building the Demo application ##

1.	Download [XAP](http://www.gigaspaces.com/LatestProductVersion)
2.	[Install XAP](http://docs.gigaspaces.com/xap100/installation.html)
3.	Install Maven and the [XAP Maven plug-in](http://docs.gigaspaces.com/xap100/maven-plugin.html)
4.	Download the application [source code](https://github.com/fe2s/xap-spark)
5.	Build a project by running `mvn clean install`

## <a name="deploying-xap-space-and-web-pu"/>Deploying XAP Space and Web PU ##

1.	Set the XAP lookup group to `spark` by adding `export LOOKUPGROUPS=spark` line to `<XAP_HOME>/bin/setenv.sh/bat`
2.	Start a Grid Service Agent by running the `gs-agent.sh/bat` script
3.	Deploy a space by running `mvn os:deploy -Dgroups=spark` from `<project_root>/word-counter-demo` directory

## <a name="launch-spark-application"/>Launch Spark Application ##

### <a name="option-a.-run-embedded-spark-cluster"/>Option A. Run embedded Spark cluster ###

This is the simplest option that doesn't require downloading and installing Spark distributive, which is useful for the development purposes. Spark runs in the embedded mode with as many worker threads as logical cores on your machine.

1.	Navigate to the `<project_root>/word-counter-demo/spark/target` directory
2.	Run the following command `java -jar spark-wordcounter.jar -s jini://*/*/space?groups=spark -m local[*]`

### <a name="option-b.-run-spark-standalone-mode-cluster"/>Option B. Run Spark standalone mode cluster ###

In this option Spark runs a cluster in the standalone mode (as an alternative to running on a Mesos or YARN cluster managers).

#### <a name="run-spark"/>Run Spark ###

1.	Download Spark (tested with Spark 1.2.1 pre-built with Hadoop 2.4)
2.	Follow [instructions](http://spark.apache.org/docs/1.2.0/spark-standalone.html) to run a master and 2 workers. Here is an example of commands with hostname `fe2s` (remember to substitute it with yours)
```
	./sbin/start-master.sh
	./bin/spark-class org.apache.spark.deploy.worker.Worker spark://fe2s:7077
	./bin/spark-class org.apache.spark.deploy.worker.Worker spark://fe2s:7077
```

#### <a name="submit-application"/>Submit application ####

1.	Submit an application to Spark (in this example driver runs locally)
2.	Navigate to the `<project_root>/word-counter-demo/spark/target` directory
3.	Run `java -jar spark-wordcounter.jar -s jini://*/*/space?groups=spark -m spark://fe2s:7077 -j ./spark-wordcounter.jar`
4.	Spark web console should be available at [http://fe2s:8080](http://fe2s:8080)

## <a name="launch-feeder-application"/>Launch Feeder application ##

1.	Navigate to `<project_root>/word-counter-demo/feeder/target`
2.	Run `java -jar feeder.jar -g spark -n 500`


At this point all components should be up and running. The application is available at [http://localhost:8090/web/](http://localhost:8090/web/)
