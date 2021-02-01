package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: 基于canal同步mysql数据，从kafka中将数据取出，并且根据表名，将数据进行分流，发送到不同的kafka topic中
 * @Author: msz
 * @UpdateTime: 2020/11/23 5:28
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "gmall0621_db_c"
    val groupId = "base_db_canal_group"
    //    从redis中读取偏移量
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    //   根据偏移量消费kafka中的数据
    if (offset != null && offset.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offset)
    } else {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }
    //    获取当前采集周期对kafka主题消费的偏移量情况
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

    //    根据表名，将数据发送到kafka不同的topic中
    jsonObjDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            //            获取类型
            val opType: String = jsonObj.getString("type")
            val tableName: String = jsonObj.getString("table")
            val dataObj: JSONArray = jsonObj.getJSONArray("data")
            if(dataObj!=null && !dataObj.isEmpty){
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
                //5.3 拼接发送到kafka的主题名
                var sendTopic :String = "ods_" + tableName
                //5.4 发送消息到kafka
                MyKafkaSink.send(sendTopic,dataObj.toString)
              }
            }
          }
        }
        //    保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
