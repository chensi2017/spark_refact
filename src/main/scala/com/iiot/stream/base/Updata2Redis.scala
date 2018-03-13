package com.iiot.stream.base

import scala.collection.mutable

object Updata2Redis {
  val hashMapTotalNum = new mutable.HashMap[String,Long]()
  val hashMapTid = new mutable.HashMap[String,Long]()
  val hashMapDate = new mutable.HashMap[String,Long]()
  val hashMapTidDate = new mutable.HashMap[String,Long]()

  val flag= 0
  val handle0 =  RedisOperation.getResource()
  handle0.set("htdata:flag", flag.toString)


  def getMapTotalNum ={
    hashMapTotalNum
  }

  def getMapTiD ={
    hashMapTid
  }
  def getMapDate ={
    hashMapDate
  }
  def getMapTidDate ={
    hashMapTidDate
  }

  def updata2Redis: Unit ={
   val handle =  RedisOperation.getResource()
    //1,total
    hashMapTotalNum.keys.foreach(x => {
      val num = hashMapTotalNum.get(x) match {
        case Some(a) =>a
        case None => 0
      }
    handle.incrBy(x,num)
    })

    //2,tid
    hashMapTid.keys.foreach(x => {
      val num = hashMapTid.get(x) match {
        case Some(a) =>a
        case None => 0
      }
      handle.incrBy(x,num)
    })

    //3,date
    hashMapDate.keys.foreach(x => {
      val num = hashMapDate.get(x) match {
        case Some(a) =>a
        case None => 0
      }
      handle.incrBy(x,num)
    })

    //4,tid and date
    hashMapTidDate.keys.foreach(x => {
      val num = hashMapTidDate.get(x) match {
        case Some(a) =>a
        case None => 0
      }
      handle.incrBy(x,num)
    })

  }

}
