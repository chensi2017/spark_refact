package com.iiot.stream.util

import com.iiot.stream.spark.HTStateContext

/**
  * Created by yuxiao on 2017/7/5.
  * script for run HTMonitorContext and HTStateContext
  */
object HTStreamAPP extends App{
  val context = new HTStateContext
  context.run()
  context.run("master","appName")
  println("state context")
  println("this is app")
}
