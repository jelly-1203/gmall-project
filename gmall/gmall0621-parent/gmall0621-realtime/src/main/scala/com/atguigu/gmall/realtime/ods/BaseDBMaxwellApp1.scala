package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBMaxwellApp1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("maxwell").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "gmall0621_db_m"
    val groupId = "base_db_maxwell_group"
    //    1.获取偏移量
    var inputStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null) {
      inputStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    } else {
      inputStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }
    //    2.获取获取偏移量范围
    var offsetRanges: Array[OffsetRange] = null
    val transformStream: DStream[ConsumerRecord[String, String]] = inputStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //    3.改变数据结构将ConsumerRecord =》 JsonObject
    val jsonObjStream: DStream[JSONObject] = transformStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    jsonObjStream.foreachRDD{
      rdd => {
        rdd.foreach{
          jsonObj => {
            val opType: String = jsonObj.getString("type")
            val tableName: String = jsonObj.getString("table")
            val dataJson: JSONObject = jsonObj.getJSONObject("data")
            if(dataJson != null && !dataJson.isEmpty){
              if(
                ("order_info".equals(tableName)&&"insert".equals(opType))
                  || (tableName.equals("order_detail") && "insert".equals(opType))
                  ||  tableName.equals("base_province")
                  ||  tableName.equals("user_info")
                  ||  tableName.equals("sku_info")
                  ||  tableName.equals("base_trademark")
                  ||  tableName.equals("base_category3")
                  ||  tableName.equals("spu_info")

              ){
                var sendTopic = "ods_"+tableName
                MyKafkaSink.send(sendTopic,dataJson.toString())
              }

            }
            /* if(!"delete".equals(opType)){
               var sendTopic = "ods_" + tableName
               MyKafkaSink.send(sendTopic,dataJson.toString())
             }*/
          }
        }
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start();
    ssc.awaitTermination();
  }
}
