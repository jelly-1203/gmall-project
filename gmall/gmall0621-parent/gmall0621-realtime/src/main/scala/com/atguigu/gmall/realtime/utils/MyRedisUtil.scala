package com.atguigu.gmall.realtime.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtil {
  var jedisPool: JedisPool = null

  def getJedis: Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  def build(): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    val host: String = prop.getProperty("redis.host")
    val port: String = prop.getProperty("redis.port")
    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100)
    jedisPoolConfig.setMaxIdle(20)
    jedisPoolConfig.setMinIdle(20)
    jedisPoolConfig.setBlockWhenExhausted(true)
    jedisPoolConfig.setMaxWaitMillis(5000)
    jedisPoolConfig.setTestOnBorrow(true)
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  //  测试
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = MyRedisUtil.getJedis
    println(jedis.ping())
    jedis.close()
  }
}
