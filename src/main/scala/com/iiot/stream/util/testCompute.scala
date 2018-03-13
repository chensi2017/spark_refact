package com.iiot.stream.util

import scala.io.Source
import scala.collection.mutable.HashMap
import java.util.HashSet

import com.iiot.stream.base.DPUnion
import com.iiot.stream.tools.TimeTransform

import scala.collection.mutable

object testCompute {

  def main(args: Array[String]): Unit = {
    //1,DP总数
    var numTotalDP = 0
    //2,按tid统计dp总数
    var hashMapByTid= new HashMap[String,Int]
    //3,按date统计dp总数
    var hashMapByDate= new HashMap[String,Int]
    //4,按date和tid统计dp总数
    var hashMapByTidDate = new HashMap[Tuple2[String,String],Int]
    //5,去重的DN数
    var hashSetDN = new HashSet[String]()
    //6,去重的thingid
    var hashSetThingID = new HashSet[String]()

    //7,去重的thingid  按日期统计个数
    var hashMapThingIDByDate = new HashMap[String,Int]
    //按thingid 去重   【thingid,date】
    var hashMapDistinctThingIDByDate = new HashMap[String,String]()

    //8,去重的thingid  按tid和date统计个数
    var hashMapThingIDByTidDate = new HashMap[Tuple2[String,String],Int]
    //按thingID 去重   【thing ,[tid,date]】
    var hashMapDistinctThingIDByTidDate = new HashMap[String,Tuple2[String,String]]

    //9,去重的compantID  按tid和date统计个数
    var hashMapComponantIDByTidDate = new HashMap[Tuple2[String,String],Int]
    //按compantID 去重   【componant ,[tid,date]】
    var hashMapDistinctComponantIDByTidDate = new HashMap[String,Tuple2[String,String]]

    Source.fromFile("C:\\codes\\git_codes\\refactor\\stream_refactor\\stream_refact\\src\\main\\resources\\dp.txt" )
      .getLines().foreach {
       x => {
         //tid:2 ts:1504772139154 compid:10009 thingid:100003 dn:AAAAAQAAABAAAAAAAANqzg==9
         var arr = x.toString.split(" ")
         val dp =new DPUnion
         dp.setTid(arr(0).split(":")(1))
         dp.setTs(arr(1).split(":")(1).toLong)
         dp.setCompId(arr(2).split(":")(1))
         dp.setThingId(arr(3).split(":")(1))
         dp.setDnStr(arr(4).split(":")(1))
         val date = TimeTransform.timestampToDate(dp.getTs)

         //1,统计所有DP总数
         numTotalDP = numTotalDP + 1

         //2，按租户统计DP
         val tid =  hashMapByTid.getOrElse(dp.getTid,0)
         hashMapByTid.put(dp.getTid ,tid + 1)

         //3，按日期统计DP
         val dateNum =  hashMapByDate.getOrElse(date,0)
          hashMapByDate.put(date,dateNum + 1)

         //4，dp num by tid and date
         val tidDateNum= hashMapByTidDate.getOrElse((dp.getTid,date),0)
         hashMapByTidDate.put((dp.getTid,date),tidDateNum + 1)

         //5，所有独立DN数据
         hashSetDN.add(dp.getDnStr)

         //6，所有独立设备数据
         hashSetThingID.add(dp.getThingId)

         //7，distinct thingID num by date
         hashMapDistinctThingIDByDate.put(dp.getThingId,date)

         //8，distinct thingID num by tid and date
         hashMapDistinctThingIDByTidDate.put(dp.getThingId,(dp.getTid,date))

         //9，distinct componantID by tid and date
         hashMapDistinctComponantIDByTidDate.put(dp.getCompId,(dp.getTid,date))
       }
    }

    //1,total dp
    println("the total  DP num is:" + numTotalDP)

    //2,total dp by tid
    hashMapByTid.keys.foreach(x => {
      print( "dp by tid Key = " + x )
      println(" Value = " + hashMapByTid.get(x) )
    })

    //3,total dp by date
    hashMapByDate.keys.foreach(x =>{
      print( "dp by date Key = " + x )
      println(" Value = " + hashMapByDate.get(x) )
    })

    //4,dp num by tid and date
    hashMapByTidDate.keys.foreach(x =>{
      print( "dp by tid and  date tid  = " + x._1 + "   date = " + x._2)
      println(" Value = " + hashMapByTidDate.get(x) )
    })

    println()
    println("-------------------the flowing is distinct items--------------------")
    println()
    //5, distinct dn
    println("distinct dn num is:" + hashSetDN.size)
    //6, distinct thingID
    println("distinct thingID num is:" + hashSetThingID.size)


    //7, distinct thingID by date
    var itDate = hashMapDistinctThingIDByDate.iterator
    var thingIDNumByDate = 0
    while (itDate.hasNext){
      val dp  = itDate.next()
      thingIDNumByDate = thingIDNumByDate + 1
      val dateNum = hashMapThingIDByDate.getOrElse(dp._2,0)
      hashMapThingIDByDate.put(dp._2,dateNum + 1)
    }

    //7，distinct thingID num by date
     hashMapThingIDByDate.keys.foreach(x =>{
    print( "distinct thingID by date = " + x )
    println(" Value = " + hashMapThingIDByDate.get(x) )
    })


    //8, distinct thingID by date and tid
    var itthingTidDate = hashMapDistinctThingIDByTidDate.iterator
    var thingIDNumByTidDate = 0
    while (itthingTidDate.hasNext){
      val dp  = itthingTidDate.next()
      thingIDNumByTidDate = thingIDNumByTidDate + 1
      val dateNum = hashMapThingIDByTidDate.getOrElse((dp._2._1,dp._2._2),0)
      hashMapThingIDByTidDate.put((dp._2._1,dp._2._2),dateNum + 1)
    }

    //8，distinct thingID num by date
    hashMapThingIDByTidDate.keys.foreach(x =>{
      print( "distinct thingID by tid and tid = " + x._1 + "date =" + x._2)
      println(" Value = " + hashMapThingIDByTidDate.get(x) )
    })


    //9, distinct componantID by date and tid
    var itCompTidDate = hashMapDistinctComponantIDByTidDate.iterator
    var componantIDNumByTidDate = 0
    while (itCompTidDate.hasNext){
      val dp  = itCompTidDate.next()
      componantIDNumByTidDate = componantIDNumByTidDate + 1
      val dateNum = hashMapComponantIDByTidDate.getOrElse((dp._2._1,dp._2._2),0)
      hashMapComponantIDByTidDate.put((dp._2._1,dp._2._2),dateNum + 1)
    }

    //9，distinct componantID num by date
    hashMapComponantIDByTidDate.keys.foreach(x =>{
      print( "distinct componantID by tid and tid = " + x._1 + "date =" + x._2)
      println(" Value = " +hashMapComponantIDByTidDate.get(x) )
    })

    }
}
