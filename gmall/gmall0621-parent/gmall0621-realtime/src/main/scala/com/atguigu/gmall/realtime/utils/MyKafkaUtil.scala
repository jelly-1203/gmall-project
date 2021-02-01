package com.atguigu.gmall.realtime.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object MyKafkaUtil {
  private val prop: Properties = MyPropertiesUtil.load("config.properties")
  private val broker_list: String = prop.getProperty("kafka.broker.list")
  var kafkaParam = collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall0621_group",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:java.lang.Boolean))
  def getKafkaDStream(topic:String,ssc:StreamingContext) = {
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    inputDStream
  }
  def getKafkaDStream(topic:String,ssc:StreamingContext,groupId:String) = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG)=groupId
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    inputDStream
  }
  def getKafkaDStream(topic:String,ssc:StreamingContext,groupId:String,offsets:Map[TopicPartition,Long]) = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam,offsets)
    )
    inputDStream
  }
}
