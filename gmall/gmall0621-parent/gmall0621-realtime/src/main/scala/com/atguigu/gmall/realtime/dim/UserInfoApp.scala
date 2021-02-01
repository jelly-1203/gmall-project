package com.atguigu.gmall.realtime.dim

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Date
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topic: String = "ods_user_info"
    val groupId = "gmall_user_info_group"

//    读取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    //    根据偏移量消费kafka消息
    if(offsetMap != null && offsetMap.size > 0){
      inputDStream =  MyKafkaUtil.getKafkaDStream(topic,ssc,groupId,offsetMap)
    }else{
      inputDStream = MyKafkaUtil.getKafkaDStream(topic,ssc,groupId)
    }
//    获取消费范围

    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      }
        rdd
    }
//    改变结构，将ConsumerRecord =》UserInfo
    val userInfoDStream: DStream[UserInfo] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])

        /*
        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val  betweenMs = curTs - date.getTime
        val age = betweenMs/1000L/60L/60L/24L/365L
        if(age<20){
          userInfo.age_group="20岁及以下"
        }else if(age>30){
          userInfo.age_group="30岁以上"
        }else{
          userInfo.age_group="21岁到30岁"
        }
        if(userInfo.gender=="M"){
          userInfo.gender_name="男"
        }else{
          userInfo.gender_name="女"
        }
        */


        userInfo
      }
    }
//    将数据存储到HBase中
    import org.apache.phoenix.spark._
    userInfoDStream.foreachRDD{
      rdd => {
        rdd.saveToPhoenix(
          "GMALL0621_USER_INFO",
          Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")

        )
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
