package com.atguigu.gmall.realtime.utils

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @Description: 对MySQL中的偏移量进行管理
 * @Author: msz
 * @UpdateTime: 2020/11/18 14:10
 */
object MySQLOffsetManagerUtil {
/*  def getOffset(topic:String,groupId:String):Map[TopicPartition,Long] = {
    val rsMap:mutable.Map[TopicPartition,Long] = null
    val sql:String = s"select group_id,topic,partition_id,topic_offset from offset_0621 where topic = '${topic}' and group_id = '${groupId}'"
    val rsList: List[JSONObject] = MySQLUtil.queryList(sql)
    val mapList: List[(TopicPartition, Long)] = rsList.map {
      jsonObj => {
        val groupId: String = jsonObj.getString("group_id")
        val topic: String = jsonObj.getString("topic")
        val partition: Int = jsonObj.getIntValue("partition_id")
        val offset: Long = jsonObj.getLongValue("topic_offset")
        (new TopicPartition(topic, partition), offset)
      }
    }
    for ((partition,offset) <- mapList) {
      rsMap.put(partition,offset)
    }

    rsMap.toMap
  }*/

  def getOffset(topic:String,groupId:String): Map[TopicPartition, Long] ={
    var sql:String = s"select topic,group_id,partition_id,topic_offset from offset_0621 where topic='${topic}' and group_id='${groupId}'"
    val offsetJsonObjList: List[JSONObject] = MySQLUtil.queryList(sql)

    val offsetMap: Map[TopicPartition, Long] = offsetJsonObjList.map {
      offsetJsonObj => {
        //{"topic":"","group_id":"","partition_id":xx,"topic_offset":""}
        val partitionId: Int = offsetJsonObj.getIntValue("partition_id")
        val offsetVal: Long = offsetJsonObj.getLongValue("topic_offset")
        (new TopicPartition(topic, partitionId), offsetVal)
      }
    }.toMap
    offsetMap

  }

}
