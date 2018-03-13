package com.iiot.stream.spark

import com.htiiot.common.config.imp.ConfigClient
import com.iiot.stream.base.{DPList, DPUnion, HTBaseContext}
import com.iiot.stream.tools.{HTInputDStreamFormat, MemConfig, ZookeeperClient}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}



/**
  * Created by yuxiao on 2017/7/5.
  */
class HTMonitorContext extends HTBaseContext{

  override def setExecutor(stream: DStream[(String, String)]): Unit = {
    //val jsonDStream:DStream[DPUnion] = HTInputDStreamFormat.inputDStreamFormat(stream)
    val jsonDStream:DStream[DPUnion] = HTInputDStreamFormat.inputDStreamFormatWithDN(stream)

    //统计
    val statistics=new HTStateStatisticsFewerReduceextends
    statistics.DPStatistics(jsonDStream)
    //statistics.distinctDnThingID(jsonDStream)

    //监控
    val monitorOperation =new HTMonitorOperation
    monitorOperation.monitor(jsonDStream)

  }
}

object HTMonitorContext{
  def main(args: Array[String]): Unit = {
    val zkClent = new ZookeeperClient
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val zk = zkClent.getConfingFromZk("172.26.223.113:2181", 30000)
    var configs = zkClent.getAll(zk, "/conf_htiiot/spark_streamming")
    val context = new HTMonitorContext
    if(configs.get("spark.run.type").equals("cluster")) {
      context.run(configs.getProperty("spark.master"),configs.getProperty("spark.appHMonitorName"))
    }else {
      context.run()
    }

  }
}
