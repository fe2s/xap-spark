# Introduction #

Real-time processing is becoming more and more popular. Spark Streaming is an extension of the core Spark API that allows scalable, high-throughput, fault-tolerant stream processing of live data streams.

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

```scala
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
