package com.atguigu.gmall.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @Description: 对Redis中的偏移量进行管理
 * @Author: msz
 * @UpdateTime: 2020/11/18 14:10
 */
object OffsetManagerUtil {

/*  //  从Redis中获取偏移量
  //  type:hash key : offset:topic:groupId filed: partition value: 偏移量
  //  返回值与kafkautil方法中的offsets一致！！！！
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    //    获取Jedis客户端
    val jedis: Jedis = MyRedisUtil.getJedis
    //  拼接操作Redis的key
    var offsetKey = s"offset:${topic}:${groupId}"
    val partitionAndOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //  对得到的信息进行结构转换
    //  先将java的map转换成scala的map
    import scala.collection.JavaConverters._
    //  对redis中读取到的数据进行结构转换
    val offsetMap: Map[TopicPartition, Long] = partitionAndOffsetMap.asScala.map {
      case (partition, offset) => {
        println(s"读取分区：${partition}--${offset}")
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    //  关闭jedis客户端
    jedis.close()
    offsetMap
  }

  //  将偏移量信息保存到Redis中
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis: Jedis = MyRedisUtil.getJedis
    //  拼接rediskey
    var offsetKey = s"offset:${topic}:${groupId}"
    var partitionAndOffsetMap = new util.HashMap[String,String]()
//    对offsetRanges进行遍历，得到topic 分区 偏移量情况
    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      println(s"保存分区：${partition}:${fromOffset}--------${untilOffset}")
      partitionAndOffsetMap.put(partition.toString,untilOffset.toString)
    }
    jedis.hmset(offsetKey,partitionAndOffsetMap)
    jedis.close()
  }*/

//  从Redis在中获取offset，从kafka中的对应的offset读取
//  offset:topic:group
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long] = {
  val jedis: Jedis = MyRedisUtil.getJedis
  var redisKey:String = s"offset:${topic}:${groupId}"
  val partitionOffsets: util.Map[String, String] = jedis.hgetAll(redisKey)
  import scala.collection.JavaConverters._
  val offsetMap: Map[TopicPartition, Long] = partitionOffsets.asScala.map {
    case (partition, offset) => {
      println(s"读取分区：${partition}--偏移量：${offset}")
      (new TopicPartition(topic, partition.toInt), offset.toLong)
    }
  }.toMap
  jedis.close()
  offsetMap
}
//  手动提交offset
  def  saveOffset(topic:String,groupId:String,offsetRanges: Array[OffsetRange]): Unit ={
    val jedis: Jedis = MyRedisUtil.getJedis
    var redisKey:String = s"offset:${topic}:${groupId}"
    var partitionAndOffsetMap = new util.HashMap[String,String]()
    for (offset <- offsetRanges) {
      val fromOffset: Long = offset.fromOffset
      val untilOffset: Long = offset.untilOffset
      val partition: Int = offset.partition
      println(s"保存分区：${partition}:${fromOffset}--------${untilOffset}")
      partitionAndOffsetMap.put(partition.toString,untilOffset.toString)
    }
    jedis.hmset(redisKey,partitionAndOffsetMap)
    jedis.close()
  }


}
