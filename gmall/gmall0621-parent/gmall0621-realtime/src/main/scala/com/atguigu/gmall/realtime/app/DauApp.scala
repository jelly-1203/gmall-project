package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo

import com.atguigu.gmall.realtime.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    var topic = "gmall_start_0621"
    var groupId = "gmall0621_group"
    //  从kafka中实时消费数据
//    从指定offset获取数据
//    获取偏移量
   /* val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var  inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0){
//      如果从Redis中可以获取到偏移量数据，那么从指定的偏移量位置开始消费
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId,offsetMap)
    }else{
      //      如果从Redis中没有获取到偏移量数据，那么还是从最新位置开始消费
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }
    //  把当前批次对kafka消费偏移量情况
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        //        将RDD类型对象强制转换为KafkaRDD
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }*/
//    获取offset，从指定偏移量开始消费
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var InputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0){
//      如果从Redis中获取到偏移量，则从指定的偏移量位置开始消费
      InputStream = MyKafkaUtil.getKafkaDStream(topic,ssc,groupId,offsetMap)
    }else{
//      如果没从Redis中获取偏移量，则从最新位置开始消费
      InputStream = MyKafkaUtil.getKafkaDStream(topic,ssc,groupId)
    }
    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    var offsetRanges:Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = InputStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }





    //  测试
    //    inputDStream.map(_.value()).print(1000)

    //  将结构进行转换,添加日期，小时属性
    val jsonDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        val ts: Long = jsonObj.getLongValue("ts")
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHour: String = sdf.format(new Date(ts))
        val dtHr: Array[String] = dateHour.split(" ")
        val dt: String = dtHr(0)
        val hr: String = dtHr(1)
        jsonObj.put("dt", dt)
        jsonObj.put("hr", hr)
        jsonObj
      }
    }
//    jsonDStream.print(1000)
    //  按需求过滤日志,获取新增用户
    //    两种方式
    //    方式二
    val filterDStream: DStream[JSONObject] = jsonDStream.mapPartitions {
      jsonObjIter => {
//        println(jsonObjIter)
        val firstList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        val jedis: Jedis = MyRedisUtil.getJedis
        for (jsonObject <- jsonObjIter) {
          val dt: String = jsonObject.getString("dt")
          val hr: String = jsonObject.getString("hr")
          val mid: String = jsonObject.getJSONObject("common").getString("mid")
          val daukey: String = "dau:" + dt
          val isNotExists: lang.Long = jedis.sadd(daukey, mid)
          if (jedis.ttl(daukey) < 0) {
            jedis.expire(daukey, 3600 * 24)
          }
          if (isNotExists == 1L) {
            firstList.append(jsonObject)
          }
        }
        jedis.close()
        firstList.toIterator
      }
    }
//    filterDStream.count().print(100)
    //  将日志插入到ES中，批量插入
    filterDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          jsonObjectIter => {
            val dauList: List[(String, DauInfo)] = jsonObjectIter.map {
              jsonObj => {
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
               /* val dauInfo = DauInfo(
                  commonObj.getString("mid"),
                  commonObj.getString("uid"),
                  commonObj.getString("ar"),
                  commonObj.getString("ch"),
                  commonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLongValue("ts")
                )*/
                val dauInfo: DauInfo = JSON.toJavaObject(jsonObj,classOf[DauInfo])
                dauInfo.mi = "0"
                (dauInfo.mid,dauInfo)
              }
            }.toList
            val dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauList, "gmall0621_dau_info_" + dt)
          }
        }
//        将偏移量保存到Redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

