package com.atguigu.gmall.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @Description: 从Kafka中读取dwd_order_info和dwd_order_detail进行双流合并，计算实付分摊
 * @Author: msz
 * @UpdateTime: 2020/11/24 15:49
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "dwd_order_info"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "dwd_order_detail"

    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)
    //    根据偏移量到Kafka读取数据
    //    orderInfo
    var orderInfoInputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0) {
      orderInfoInputDStream = MyKafkaUtil.getKafkaDStream(orderInfoTopic, ssc, orderInfoGroupId, orderInfoOffsetMap)
    } else {
      orderInfoInputDStream = MyKafkaUtil.getKafkaDStream(orderInfoTopic, ssc, orderInfoGroupId)
    }
    //    orderDetail
    var orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0) {
      orderDetailInputDStream = MyKafkaUtil.getKafkaDStream(orderDetailTopic, ssc, orderDetailGroupId, orderDetailOffsetMap)
    } else {
      orderDetailInputDStream = MyKafkaUtil.getKafkaDStream(orderDetailTopic, ssc, orderDetailGroupId)
    }

    //    orderInfo
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoInputDStream.transform {
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //    orderDetail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailInputDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //    orderInfo
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        orderInfo
      }
    }
    //    orderDetail
    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      record => {
        val jsonStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
        orderDetail
      }
    )

    //    orderInfoDStream.print(1000)
    //    orderDetailDStream.print(1000)

    //    滑窗 + 去重实现双流join
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDStream.window(Seconds(30), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDStream.window(Seconds(30), Seconds(5))

    //    将订单和订单明细DS结构进行转换，转换为kv结构
    val orderInfoWidowWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map(orderInfo => {
      (orderInfo.id, orderInfo)
    })
    val orderDetailWindowWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map(orderDetail => {
      (orderDetail.order_id, orderDetail)
    })

    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWidowWithKeyDStream.join(orderDetailWindowWithKeyDStream)
    //    去重，借助Redis set 天然去重，可以批量操作，sadd操作可以看做没做处理
    //    order_join:[orderId]  value ? orderDetailId  expire : 60*10
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        val jedis: Jedis = MyRedisUtil.getJedis
        //        定义一个新的集合，存放合并之后的数据
        val orderWideList = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          //          拼接Redis的Key
          val key = "order_join" + orderId
          //          添加进去了，则返回1，否则返回0
          val isNotExist: lang.Long = jedis.sadd(key, orderDetail.id.toString)
          //          没设置过失效时间，默认为 -1
          if (jedis.ttl(key) < 0) {
            //          第一次设置过，后续就不需要再进行设置了
            jedis.expire(key, 600)
          }
          if (isNotExist == 1L) {
            //            说明不重复，以前没有进行过订单和该明细的合并，封装一个OrderWide对象
            orderWideList.append(new OrderWide(orderInfo, orderDetail))

          }
        }

        jedis.close()
        orderWideList.toIterator
      }
    }
    //    orderWideDStream.print(1000)

    //    实付金额分摊实现
    //    Redis : type :String key:order_origin_sum:order_id valuie:累积金额 失效时间：600
    val splitOrderWideDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        //        获取Redis连接
        val jedis: Jedis = MyRedisUtil.getJedis
        //        如果只计算一次的话，可以不转换
        val orderWideList: List[OrderWide] = orderWideItr.toList
        for (orderWide <- orderWideList) {
          //        从Redis中获取处理过的明细单价*数量合计
          var originSumKey = "order_origin_sum" + orderWide.order_id
          var originSum: Double = 0D
          val originSumStr: String = jedis.get(originSumKey)
          //          注意：从Redis中取数据，都要进行非空判断
          if (originSumStr != null && originSumStr.size > 0) {
            originSum = originSumStr.toDouble
          }

          //          从Redis中获取处理过的明细分摊金额的合计
          var splitSumKey = "order_split_sum:" + orderWide.order_id
          var splitSum = 0D
          val splitSumStr: String = jedis.get(splitSumKey)
          if (splitSumStr != null && splitSumStr.size > 0) {
            splitSum = splitSumStr.toDouble
          }

          //          判断是否为最后一条
          var detailAmount = orderWide.sku_price * orderWide.sku_num
          if (detailAmount == orderWide.original_total_amount - originSum) {
            //            是最后一条 减法
            //            Math.round 四舍五入
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - splitSum) * 100D) / 100D
          } else {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100D) / 100D
          }
          //        将计算之后的累加金额保存到Redis中
          val newOriginSum = originSum + detailAmount
          jedis.setex(originSumKey, 600, newOriginSum.toString)
          val newSplitSum = splitSum + orderWide.final_detail_amount
          jedis.setex(splitSumKey, 600, newSplitSum.toString)
        }


        jedis.close()
        orderWideList.toIterator
      }
    }
//    splitOrderWideDStream.print(1000)

    /*    //    双流join
        //    采用滑窗+去重
        val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDStream.window(Seconds(30), Seconds(5))
        val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDStream.window(Seconds(30), Seconds(5))
        val orderInfoWidowWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map(orderInfo => (orderInfo.id, orderInfo))
        val orderDetailWindowWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))
        val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWidowWithKeyDStream.join(orderDetailWindowWithKeyDStream)

        //     去重
        //    将join后的数据
        //    Redis：set   key：order_join:orderId value orderDetailId
        val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
          tupleItr => {
            val jedis: Jedis = MyRedisUtil.getJedis
            val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
            val orderWideList: ListBuffer[OrderWide] = new ListBuffer[OrderWide]()
            for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
              val redisKey = "order_join:" + orderId
              val isNotExists: lang.Long = jedis.sadd(redisKey, orderDetail.id.toString)
              if (jedis.ttl(redisKey) < 0) {
                jedis.expire(redisKey, 600)
              }
              if (isNotExists == 1L) {
                //            不是重复数据
                orderWideList.append(new OrderWide(orderInfo, orderDetail))
              }
            }
            jedis.close()
            orderWideList.toIterator
          }


        //    实付分摊
        //    orderWideDStream
        //    每中sku的总原始金额 = 每个sku原始金额 * 该sku的个数
        //    originalSum = orderDetail:order_price * sku_num
        判断是否是最后一个商品
        总原始金额 - 每种sku的总原始金额的累加值 == 最后一个sku的总原始金额？
        original_total_amount - originalSum == sku_price * sku_num
          在Redis中存放商品原始金额的累加值 originalSum
          在Redis中存放分摊金额的累加值 splitSum
          if(original_total_amount - originalSum == sku_price * sku_num){
            分摊金额 = 总消费金额 - 前面分摊金额的累加值
            final_detail_amount = final_total_amount - splitSum
          }else{
            分摊金额 = 总消费金额 * 每种sku的总原始金额 / 订单的总原始金额
            orderWide: final_detail_amount = final_total_amount * sku_price * sku_num / original_total_amount
          }
            val splitOrderWideDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
              orderWideItr => {
                val jedis: Jedis = MyRedisUtil.getJedis
                val orderWideList: List[OrderWide] = orderWideItr.toList
                for (orderWide <- orderWideList) {
                  var originSumKey = "order_origin_sum" + orderWide.order_id
                  val originSumStr: String = jedis.get(originSumKey)
                  var originSum = 0D
                  if (originSumStr != null && originSumStr.size > 0) {
                    originSum = originSumStr.toDouble
                  }
                  var splitSum = 0D
                  val splitSumKey = "order_split_sum" + orderWide.order_id
                  val splitSumStr: String = jedis.get(splitSumKey)
                  if (splitSumStr != null && splitSumStr.size > 0) {
                    splitSum = splitSumStr.toDouble
                  }
                  val detailAmount = orderWide.sku_price * orderWide.sku_num
                  if (orderWide.original_total_amount - originSum == detailAmount) {
                    orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - splitSum) * 100D) / 100D
                  } else {
                    orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100D) / 100D
                  }
                  //          将结果累加到Redis
                  val newOriginalSum = originSum + detailAmount
                  val newSplitSum = splitSum + orderWide.final_total_amount
                  jedis.setex(originSumKey, 600, newOriginalSum.toString)
                  jedis.setex(splitSumKey, 600, newSplitSum.toString)

                }
                jedis.close()
                orderWideList.toIterator
              }
            }
            splitOrderWideDStream.print(1000)*/
    /*
    * 双流join：1. 缓存到Redis中
    *           2.使用滑窗，去重
    * */
    /*
        val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDStream.window(Seconds(30),Seconds(5))
        val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDStream.window(Seconds(30),Seconds(5))
        val orderInfoWinWithIdDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map(orderInfo =>{(orderInfo.id,orderInfo)})
        val orderDetailWinWithIdDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map(orderDetail => {(orderDetail.order_id,orderDetail)})
        val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWinWithIdDStream.join(orderDetailWinWithIdDStream)

    //    去重

        joinedDStream.mapPartitions{
          tupleItr => {
            val orderWideList: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
            val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
            val jedis: Jedis = MyRedisUtil.getJedis
            tupleList.foreach{
              case (orderId,(orderInfo,orderDetail)) =>{
                val redisKey =  "order_join:" + orderId
                val isNotExist: lang.Long = jedis.sadd(redisKey,orderDetail.order_id.toString)
                if(jedis.ttl(redisKey) < 0){
                  jedis.expire(redisKey,600)
                }
                if(isNotExist == 1L){
                  orderWideList.append( new OrderWide(orderInfo,orderDetail))
                }
              }
            }
            jedis.close()
            orderWideList.toIterator
          }
        }
    */


    /*
    * 实付分摊：
    * if（）
    *
    * */


//    创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("spark_session_orderWide").getOrCreate()
    import spark.implicits._
    //    保存到ClickHouse
    splitOrderWideDStream.foreachRDD {
      rdd => {
//        缓存rdd
        rdd.cache()
//        将rdd转换为DF
        val df: DataFrame = rdd.toDF()
//        调用df的方法，吧数据保存到ClickHouse
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://hadoop102:8123/default", "t_order_wide_0621", new Properties())


//        将数据写回kafka
       rdd.foreach{
         orderWide => {
           MyKafkaSink.send("dws_order_wide", JSON.toJSONString(orderWide, new SerializeConfig(true)))
         }
       }


//        保存偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
        OffsetManagerUtil.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
