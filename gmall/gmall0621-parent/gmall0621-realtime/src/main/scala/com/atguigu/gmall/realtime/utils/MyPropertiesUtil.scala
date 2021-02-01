package com.atguigu.gmall.realtime.utils

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

object MyPropertiesUtil {
  private val prop = new Properties()

  def load(filePath: String): Properties = {
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(filePath), StandardCharsets.UTF_8))
    prop
  }

  //测试
  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
    
  }
}
