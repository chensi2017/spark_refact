package com.iiot.stream.spark

import com.htiiot.store.model.{DataPoint, Metric}
import com.iiot.stream.base.{DPUnion, EventList}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer


/**
  * Created by yuxiao on 2017/9/19.
  */
class HTMonitorOperation extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTMonitorOperation])

  val hashMap = EventList.getEventMap
  val threadNum = 4

  val foreachPartitionFunc = (it: Iterator[DPUnion]) => {
    val startT = System.currentTimeMillis()
    var arrTotal = new ArrayBuffer[DataPoint]()
    try {
      while (it.hasNext) {
        val UP = it.next()
        var DP = new DataPoint
        DP.setDeviceNumber(UP.getDn)
        DP.setTs(UP.getTs)
        try {
          DP.setMetric(new Metric(UP.getKey, UP.getValue.toDouble.formatted("%.2f").toDouble))
        } catch {
          case e: NumberFormatException => {
          }
        }
        arrTotal += DP
      }
    }catch {
      case e:NullPointerException =>{
        logger.error("NullPointer Error"+e.getMessage)
      }
      case e:NumberFormatException =>{
        logger.error("NumberFormat Error"+ e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error"+ e.getMessage)
      }
    }

    try {
      var start = 0
      val step = Math.floorDiv(arrTotal.length, threadNum)
      val threadbuff = ArrayBuffer[Thread]()
      if (0 == arrTotal.length) {
      }
      else {
        for (i <- 1 to threadNum) {
          val flag = if (i == threadNum) 1 else 0
          val arrSize = step + (arrTotal.length - (threadNum) * step) * flag
          val arr = new Array[DataPoint](arrSize)
          arrTotal.copyToArray(arr, start, arrSize)
          start = start + step
          val thread = new MyThread("thread" + i, i, arr, hashMap)
          threadbuff += thread
          thread.start()
        }
        for (thread <- threadbuff) {
          thread.join()
        }
      }
    }catch {
      case e:IllegalThreadStateException =>{
        logger.error("IllegalThreadState Error"+ e.getMessage)
      }
      case e:ArrayIndexOutOfBoundsException =>{
        logger.error("ArrayIndexOutOfBounds Error"+ e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error"+ e.getMessage)
      }
    }
    val endT = System.currentTimeMillis()
  }


  def monitor(jsonData: DStream[DPUnion]) = {
    jsonData.foreachRDD(rdd => {
      rdd.foreachPartition(foreachPartitionFunc)
    })
  }

}

