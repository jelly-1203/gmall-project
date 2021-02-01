package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.ProvinceInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ProvinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_province"
    val groupId = "province_info_group"

    //    从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    //    根据偏移量从kafka中读取数据
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    } else {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }
    //    获取kafka主题分区的消费情况
    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      }
        rdd
    }
//    结构转换ConsumerRecord =》 ProvinceInfo
    val provinceInfoDStream: DStream[ProvinceInfo] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        //        将json格式字符串转换成ProvinceInfo对象
        val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
        provinceInfo
      }
    }
//    保存DS数据到Phoenix
    import org.apache.phoenix.spark._
    provinceInfoDStream.foreachRDD{
      rdd => {
        rdd.saveToPhoenix(
          "GMALL0621_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
//        保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
