package com.atguigu.gmall.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.OrderWide
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, MySQLOffsetManagerUtil}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
 * @Description: 从kafka的dws_orser_wide中读取数据，对品牌交易额进行统计
 * @Author: msz
 * @UpdateTime: 2020/11/28 10:14
 */
object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    // 加载流
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("TrademarkStatApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "ads_trademark_stat_group"
    val topic = "dws_order_wide"

    //    获取偏移量
    val offsetMap: Map[TopicPartition, Long] = MySQLOffsetManagerUtil.getOffset(topic, groupId)

    //    根据偏移量到Kafka中读取数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId, offsetMap)
    } else {
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //    结构转换
    val orderWideDStream: DStream[OrderWide] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val orderWide: OrderWide = JSON.parseObject(jsonStr, classOf[OrderWide])
        orderWide
      }
    }
    orderWideDStream.print(1000)
    //    根据需求做聚合操作
    val mapDStream: DStream[(String, Double)] = orderWideDStream.map {
      orderWide => {
        //        val stat_time: String = orderWide.create_time
        val tm_id: Long = orderWide.tm_id
        val tm_name: String = orderWide.tm_name
        val final_detail_amount: Double = orderWide.final_detail_amount
        val tm = tm_name + "_" + tm_id
        (tm, final_detail_amount)
      }
    }
    val reduceDSteam: DStream[(String, Double)] = mapDStream.reduceByKey(_ + _)
//    reduceDSteam.print(1000)
    //    将数据保存到ads层 即Mysql中
    //    先将数据拉取到Driver上，避免分布式事务

    reduceDSteam.foreachRDD {
      rdd => {
        val tmSumArr: Array[(String, Double)] = rdd.collect()
     /*   if (tmSumArr != null && tmSumArr.size > 0) {
          DBs.setup()
          DB.localTx(
            implicit session => {
              val sdf = new SimpleDateFormat("yyyy-MM-dd")
              val date: String = sdf.format(new Date())
              for ((tm,amount) <- tmSumArr) {
                val tm_name= tm.split("_")(0)
                val tm_id = tm.split("_")(1)
                val tmAmount = Math.round(amount * 100D) / 100D
                //              SQL1
                println("业务数据写入操作")
                SQL ("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                  .bind(date,tm_id,tm_name,tmAmount)
              }

              //              SQL2
              println("提交偏移量")
              for (offset <- offsetRanges) {
                val partition: Int = offset.partition
                val untilOffset: Long = offset.untilOffset
                SQL("replace into offset_0621 values(?,?,?,?)")
                  .bind(groupId,topic,partition,untilOffset).update().apply()
              }
            }
          )
        }*/
//        方式二：
        if(tmSumArr != null && tmSumArr.size > 0) {
          DBs.setup()
          DB.localTx(
            implicit session => {
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val date: String = sdf.format(new Date())
              for ((tm,amount) <- tmSumArr) {
                val tm_name: String = tm.split("_")(0)
                val tm_id: String = tm.split("_")(1)
                val tmAmount: Double = Math.round(amount * 100D)/100D
                val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
                batchParamsList.append(Seq(date,tm_id,tm_name,tmAmount))
                //val params: Seq[Seq[Any]] = Seq(Seq("2020-08-01 10:10:10","101","品牌1",2000.00),
                // Seq("2020-08-01 10:10:10","102","品牌2",3000.00))
                //数据集合作为多个可变参数 的方法 的参数的时候 要加:_*
                println("插入业务数据")
                SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                  .batch(batchParamsList:_*).apply()
              }
//              向Mysql中提交偏移量
              println("提交偏移量")
              for (offset <- offsetRanges) {
                val untilOffset: Long = offset.untilOffset
                val partition: Int = offset.partition
                SQL("replace into offset_0621 values(?,?,?,?)")
                  .bind(topic,groupId,partition,untilOffset).update().apply()
              }
            }
          )

        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
