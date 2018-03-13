package com.iiot.stream.base

import java.util.HashMap


object EventList {
  val hashMap = new HashMap[String ,String]

  def getEventList {
    val handle = RedisOperation.getResource()
    val keySet  = handle.keys("event_*")
    val it = keySet.iterator()
    while(it.hasNext) {
      val key = it.next()
      val result = handle.get(key)
      hashMap.put(key, result)
    }
    handle.close()
  }

  def getEventMap={
   if(hashMap.isEmpty){
     getEventList
   }
    hashMap
  }


}
