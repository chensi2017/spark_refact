package com.iiot.stream.spark

import com.iiot.stream.base.HTBaseContext
import com.iiot.stream.tools.{HTInputDStreamFormat, MemConfig}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream

class HTStateFewerReduce extends HTBaseContext {
  @transient lazy  val logger: Logger = Logger.getLogger(HTStateContext.getClass)

  /**
    * 统计一些指标
    */
  override def setExecutor(stream: DStream[(String, String)]): Unit = {
    val stateStatistics = new HTStateStatisticsFewerReduceextends
    val jsonData = HTInputDStreamFormat.inputDStreamFormat(stream) //.persist(StorageLevel.MEMORY_AND_DISK_2)
    println("-----------------------------------------in setExecutor")


    //1,base
    stateStatistics.DPStatistics(jsonData)

    //2,dn & thingid
    stateStatistics.distinctDnThingID(jsonData)


  }
}

object HTStateFewerReduce {
  def main(args: Array[String]): Unit = {
    val configs = new MemConfig("spark_streamming.properties")
    val context = new HTStateFewerReduce

    if(configs.get("spark.run.type").equals("cluster")) {
      context.run(configs.get("spark.master"), configs.get("spark.appHStateName"))
    }else{
      context.run()
    }
  }
}
