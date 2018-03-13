package com.iiot.stream.base

import com.iiot.stream.tools.{DeviceNumber, ZookeeperClient}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by yuxiao on 2017/7/5.
  * basic class of other implement class which inherit it.to "dont repeat yourself"
  */

abstract class HTBaseContext {
  var stream: DStream[(String, String)] = _
  var scc: StreamingContext = _
  var sparkConf: SparkConf = _
  var topic: Set[String] = _
  var kafkaParam: Map[String, String] = _
  val zkClent = new ZookeeperClient
  val zk = zkClent.getConfingFromZk("172.26.223.113:2181", 30000)
  var configs = zkClent.getAll(zk, "/conf_htiiot/spark_streamming")

  def initKafkaParamters(): Unit = {
    kafkaParam = Map("metadata.broker.list" -> configs.getProperty("kafka.broker.list"),
      "group.id" -> configs.getProperty("kafka.group.id"),
      "zookeeper.connect" -> configs.getProperty("zookeeper.list"),
      "auto.offset.reset" -> configs.getProperty("auto.offset.reset"),
      "queued.max.message.chunks" -> configs.getProperty("queued.max.message.chunks"),
      "fetch.message.max.bytes" -> configs.getProperty("fetch.message.max.bytes"),
      "num.consumer.fetchers" -> configs.getProperty("num.consumer.fetchers"),
      "socket.receive.buffer.bytes" -> configs.getProperty("socket.receive.buffer.bytes")
    )


    topic = Set(configs.getProperty("kafka.topic").toString)
    println("the topic is:"+ topic)
  }


  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]): DStream[(String, String)] = {
     val numStreams = Integer.parseInt(configs.getProperty("inputStream.numStreams"))
     val kafkaStreams =
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
         .repartition(Integer.parseInt(configs.getProperty("repartion.num")))
    kafkaStreams
  }

  final def getStream: DStream[(String, String)] = {
    initKafkaParamters()
    sparkConf = new SparkConf().setMaster("local[5]").setAppName("local-spark-consumer")
      .set("spark.cores.max", configs.getProperty("spark.cores.max"))
      .set("spark.executor.cores", configs.getProperty("spark.executor.cores"))
      .set("spark.network.timeout", configs.getProperty("spark.network.timeout"))
      .set("spark.executor.memory", configs.getProperty("spark.executor.memory"))
      .set("spark.defalut.parallelism", configs.getProperty("spark.defalut.parallelism"))
      .set("spark.streaming.blockInterval", configs.getProperty("spark.streaming.blockInterval"))
      .set("spark.serializer", configs.getProperty("spark.serializer"))
        .set("spark.locality.wait.node",configs.getProperty("spark.locality.wait.node"))
      .set("spark.locality.wait",configs.getProperty("spark.locality.wait"))
      .set("spark.streaming.kafka.maxRatePerPartition", configs.getProperty("spark.streaming.kafka.maxRatePerPartition"))
      // .set("spark.streaming.stopGracefullyOnShutdown","true")
      //.set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(Array(classOf[DPList], classOf[DPListWithDN],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber]))

    scc = new StreamingContext(sparkConf, Duration(2000))
    stream = createStream(scc, kafkaParam, topic)
    stream
  }

  def getStream(master: String, appName: String): DStream[(String, String)] = {
    initKafkaParamters()
    sparkConf = new SparkConf().setMaster(master).setAppName(appName)
      .set("spark.cores.max", configs.getProperty("spark.cores.max"))
      .set("spark.executor.cores", configs.getProperty("spark.executor.cores"))
      .set("spark.network.timeout", configs.getProperty("spark.network.timeout"))
      .set("spark.executor.memory", configs.getProperty("spark.executor.memory"))
      .set("spark.defalut.parallelism", configs.getProperty("spark.defalut.parallelism"))
      .set("spark.streaming.blockInterval", configs.getProperty("spark.streaming.blockInterval"))
      .set("spark.serializer", configs.getProperty("spark.serializer"))
      .set("spark.locality.wait.node",configs.getProperty("spark.locality.wait.node"))
      .set("spark.locality.wait",configs.getProperty("spark.locality.wait"))
      .set("spark.streaming.kafka.maxRatePerPartition", configs.getProperty("spark.streaming.kafka.maxRatePerPartition"))
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(Array(classOf[DPList], classOf[DPListWithDN],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber]))
    scc = new StreamingContext(sparkConf, Duration(Integer.parseInt(configs.getProperty("duration.num"))))
    stream = createStream(scc, kafkaParam, topic)
    stream
  }

  def setExecutor(stream: DStream[(String, String)])

  def run(): Unit = {
    getStream
    setExecutor(stream)
    scc.start()
    scc.awaitTermination()
  }

  def run(master: String, appName: String): Unit = {
    getStream(master, appName)
    setExecutor(stream)
    scc.start()
    scc.awaitTermination()
  }

}