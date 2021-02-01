package com.atguigu.gmall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @Description: 从Kafka ods_order_detail中获取数据，和维度进行关联，将宽表写回kafka
 * @Author: msz
 * @UpdateTime: 2020/11/24 11:37
 */
object OrderDetailApp {
  def main(args: Array[String]): Unit = {

    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_detail";
    val groupId = "order_detail_group"


    //从redis中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量到Kafka中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId,offsetMapForKafka)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }


    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd => {
        //周期性在driver中执行
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    //提取数据
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map {
      record =>{
        val jsonString: String = record.value()
        //订单处理  转换成更方便操作的专用样例类
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
      }
    }

//    orderDetailDstream.print(1000)
//    先将商品表与其他的维度表进行关联，然后再与orderdetail表进行关联 ----------------- 维度退化

    val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions{
      orderDetailItr => {
        val orderDetailList: List[OrderDetail] = orderDetailItr.toList
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        val sql:String = s"select id,tm_id,spu_id,category3_id,tm_name,spu_name,category3_name from gmall0621_sku_info  where id in ('${skuIdList.mkString("','")}')"
        val skuInfoJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val skuInfoMap: Map[String, SkuInfo] = skuInfoJsonList.map {
          skuInfoJson => {
            val skuInfo: SkuInfo = JSON.toJavaObject(skuInfoJson, classOf[SkuInfo])
            (skuInfo.id, skuInfo)
          }
        }.toMap

        for (orderDetail <- orderDetailList) {
          val skuInfo: SkuInfo = skuInfoMap.getOrElse(orderDetail.sku_id.toString,null)
          if(skuInfo != null){
            orderDetail.spu_id = skuInfo.spu_id.toLong
            orderDetail.spu_name = skuInfo.spu_name
            orderDetail.tm_id = skuInfo.tm_id.toLong
            orderDetail.tm_name = skuInfo.tm_name
            orderDetail.category3_id = skuInfo.category3_id.toLong
            orderDetail.category3_name = skuInfo.category3_name

          }
        }
        orderDetailList.toIterator

      }
    }
//    orderDetailWithSkuDstream.print(1000)
//    将订单明细数据写回到kafka
//    Kafka只能一条条往回写数据！
    orderDetailWithSkuDstream.foreachRDD{
      rdd => {
        rdd.foreach(
          orderDetail => {
            var topicName = "dwd_order_detail"
//            !!!!!!!注意：将对象转换为json字符串的时候，需要传一个SerializeConfig
//            SerializeConfig：循环引用检查
            var msg:String = JSON.toJSONString(orderDetail,new SerializeConfig(true))
            MyKafkaSink.send(topicName,msg)

          }
        )
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
