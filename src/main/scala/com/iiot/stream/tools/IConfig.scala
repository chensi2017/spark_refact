package com.iiot.stream.tools

import java.util.Properties

/**
  * Created by yuxiao on 2017/7/5.
  */
trait IConfig {
  def hasKey(var1: String): Boolean

  def set(var1: String, var2: String): Unit

  def get(var1: String): String

  def get(var1: String, var2: String): String

  def remove(var1: String): Unit

  def getAll: Properties

}
