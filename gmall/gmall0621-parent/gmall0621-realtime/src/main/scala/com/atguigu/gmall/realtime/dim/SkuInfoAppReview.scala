/*
package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{BaseTrademark, SkuInfo}
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SkuInfoAppReview {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SkuInfoApp")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topic = "ods_sku_info";
    val groupId = "dim_sku_info_group"

//    获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    //    从kafka中消费数据
    if(offsetMap != null && offsetMap.size > 0){
     inputDStream = MyKafkaUtil.getKafkaDStream(topic,ssc,groupId,offsetMap)
    }else{
      inputDStream = MyKafkaUtil.getKafkaDStream(topic,ssc,groupId)
    }
//    记录消费的位置
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
      rdd => {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
//    转换格式
    val skuInfoDStream: DStream[Unit] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val skuInfo: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
        skuInfo
      }
    }
//    将skuInfo表与其他的维度表相关联
    skuInfoDStream.foreachRDD(
      rdd => {
        if(rdd.count()>0){
          val tmSql = "select id ,tm_name  from gmall0621_base_trademark"
          val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
          val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("ID"),jsonObj)).toMap

          // spu
          val spuSql = "select id ,spu_name  from gmall0621_spu_info" // spu
          val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
          val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"),jsonObj)).toMap

          //category3
          val category3Sql = "select id ,name from gmall0621_base_category3" //driver  周期性执行
          val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
          val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

//          将数据汇总到一个List中
          var dimList = List[Map[String,JSONObject]](tmMap,category3Map,spuMap)
          val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

          rdd.mapPartitions{
            skuInfoItr => {

            }
          }
        }
      }
    )
  }
}
*/
