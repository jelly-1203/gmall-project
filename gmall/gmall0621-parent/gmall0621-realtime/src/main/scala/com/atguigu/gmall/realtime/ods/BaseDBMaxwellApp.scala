package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "gmall0621_db_m"
    val groupId = "base_db_maxwell_group"
    //获取偏移量
     val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    //  根据偏移量到kafka中读取数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null

    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    }else{
      inputDStream =  MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }
    //  获取当前批次读取的kafka主题中分区的偏移量信息
    var offsetRanges: Array[OffsetRange] = null
     val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      }
        rdd
    }
    //    对数据进行处理，将consumerRecord ==》 Json对象
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }
    //从json对象中获取table和data，发送到不同的kafka主题
    jsonObjDStream.foreachRDD{
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
      }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
