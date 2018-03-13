package com.iiot.stream.tools

import java.util.Properties
import java.io.{File, FileInputStream}

/**
  * Created by yuxiao on 2017/7/9.
  */
class MemConfig(fileName: String) extends IConfig {
  protected var properties = new Properties
  val absolutePath: String = getPropertyPath
  println(absolutePath)
  var proFile = new FileInputStream(absolutePath + fileName)
  properties.load(proFile)

  override def set(key: String, value: String): Unit = {
    this.properties.setProperty(key, value)
  }

  override def get(key: String): String = {
    this.properties.getProperty(key)
  }

  override def get(key: String, defaultValue: String): String = {
    if (this.properties.containsKey(key)) this.properties.getProperty(key)
    else {
      this.properties.setProperty(key, defaultValue)
      defaultValue
    }
  }

  override def remove(key: String): Unit = {
    this.properties.remove(key)
  }

  override def getAll: Properties = this.properties

  override def hasKey(key: String): Boolean = this.properties.containsKey(key)

  def getPropertyPath: String = {
    val f = new File(classOf[MemConfig].getResource("/").getPath)
    var path = f.getAbsolutePath
    val separator = System.getProperty("file.separator")
    if (!path.endsWith(separator)) path += separator
    path = java.net.URLDecoder.decode(path, "utf-8")
    path
  }

}

object MemConfig {
  def main(args: Array[String]): Unit = {
    val tem = new MemConfig("spark_redis.properties")
  }

}