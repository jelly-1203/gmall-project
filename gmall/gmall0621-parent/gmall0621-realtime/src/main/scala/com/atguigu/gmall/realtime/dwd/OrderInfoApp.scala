package com.atguigu.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.atguigu.gmall.realtime.utils.{MyESUtil, MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    //1.从Kafka中查询订单信息
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group"

    //从Redis中读取Kafka偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null

    if(offsetMap!=null&&offsetMap.size>0){
      //Redis中有偏移量  根据Redis中保存的偏移量读取
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc,groupId,offsetMap)
    }else{
      // Redis中没有保存偏移量  Kafka默认从最新读取
      inputDStream = MyKafkaUtil.getKafkaDStream(topic, ssc,groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对从Kafka中读取到的数据进行结构转换，由Kafka的ConsumerRecord转换为一个OrderInfo对象
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        //获取日期赋给日期属性
        orderInfo.create_date = createTimeArr(0)
        //获取小时赋给小时属性
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }
//    orderInfoDStream.print()
    /*
    //方案1：对DStream中的数据进行处理，判断下单的用户是否为首单
    //缺点：每条订单数据都要执行一次SQL，SQL执行过于频繁
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        //通过phoenix工具到hbase中查询用户状态
        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id ='${orderInfo.user_id}'"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.size > 0) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }
    orderInfoWithFirstFlagDStream.print(1000)
    */
  /*  //方案2：对DStream中的数据进行处理，判断下单的用户是否为首单
    //优化:以分区为单位，将一个分区的查询操作改为一条SQL
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //因为迭代器迭代之后就获取不到数据了，所以将迭代器转换为集合进行操作
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区内的用户ids
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //从hbase中查询整个分区的用户是否消费过，获取消费过的用户ids
        var sql: String = s"select user_id,if_consumed from user_status0621 where user_id in('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //得到已消费过的用户的id集合
        val cosumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))

        //对分区数据进行遍历
        for (orderInfo <- orderInfoList) {
          //注意：orderInfo中user_id是Long类型，一定别忘了进行转换
          if (cosumedUserIdList.contains(orderInfo.user_id.toString)) {
            //如已消费过的用户的id集合包含当前下订单的用户，说明不是首单
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }*/
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        var sql: String = s"select user_id,if_consumed from user_status0621 where user_id in ('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
//    orderInfoWithFirstFlagDStream.print(1000)
//    同一个采集周期下单多次用户状态修正
//    第一次为首单，其余都为非首单
//    DS =》 KV类型 <user_id,orderInfo>
    val mapDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map {
      orderInfo => {
        (orderInfo.user_id, orderInfo)
      }
    }
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()
//    状态修复
    val orderInfoRealWithFirstFlagDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoItr) => {
        if (orderInfoItr.size > 1) {
          //          一个采集周期下了多个订单，需要进行状态修复
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          //          对用户下的多个订单按照创建时间进行排序（升序）
          val sortedList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          //          获取用户下的第一个订单
          val orderInfoFirst: OrderInfo = sortedList(0)
          //          判断是否标记为了首单
          //          如果第一个标记为首单了的话，其他的肯定为首单
          if (orderInfoFirst.if_first_order == "1") {
            //            对其他的订单进行状态修复，标记非首单
            for (i <- 1 until sortedList.size) {
              val orderInfoNotFirst: OrderInfo = sortedList(i)
              orderInfoNotFirst.if_first_order = "0"
            }
          }
          sortedList
        } else {
          //          正常下单，不需要进行状态修复
          orderInfoItr.toList
        }
      }
    }


//    和省份进行维度关联
//    方式一：
   /* orderInfoRealWithFirstFlagDStream.map{
      orderInfo => {
        orderInfo.province_id
      }
    }*/
/*//    方式二：以分区为单位和省份进行关联
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealWithFirstFlagDStream.mapPartitions {
      orderInfoItr => {
        //        将当前分区中的订单转换为List集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //        获取当前分区中订单的下单省份
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        //        根据省份id，到Phoenix的省份维度表中，将所有的省份信息，查询出来
        var sql: String = s"select id,name,area_code,iso_code from gmall0621_province_info where id in ('${provinceIdList.mkString("','")}')"
        val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //        因为处理的时候，不太方便，所有对JSON集合进行转换
        val provinceMap: Map[String, ProvinceInfo] = provinceJsonObjList.map {
          provinceJsonObj => {
            //            将JSON对象转换成对应样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        for (orderInfo <- orderInfoList) {
          val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
          if (provinceInfo != null) {
            orderInfo.province_name = provinceInfo.name
            orderInfo.province_area_code = provinceInfo.area_code
            orderInfo.province_iso_code = provinceInfo.iso_code
          }
        }

        orderInfoList.toIterator
      }
    }*/
//    orderInfoWithProvinceDStream.print(1000)


//    方式二：以采集周期为单位和省份进行关联
//    对Driver压力比较大，如果数据量较大的话，不采用这种方式
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealWithFirstFlagDStream.transform {
      rdd => {
        var sql: String = "select id,name,area_code,iso_code from gmall0621_province_info"
        val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceMap: Map[String, ProvinceInfo] = provinceJsonObjList.map {
          /*将json对象转换为provinceInfo样例类对象*/
          provinceJsonObj => {
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap
        //使用广播变量对Map进行封装
        val provinceMapBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)
        rdd.map {
          orderInfo => {
            val provinceInfo: ProvinceInfo = provinceMapBC.value.getOrElse(orderInfo.province_id.toString, null)
            if (provinceInfo != null) {
              orderInfo.province_name = provinceInfo.name
              orderInfo.province_area_code = provinceInfo.area_code
              orderInfo.province_iso_code = provinceInfo.iso_code
            }
            orderInfo
          }
        }
      }
    }
//    orderInfoWithProvinceDStream.print(1000)


/*
  订单和用户维度进行关联
  以分区为单位进行处理
*/
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //        获取当前分区订单对应的所有下单用户的id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //        拼接查询sql
        val sql = s"select itd,user_level,birthday,gender,age_group,gender_name from gmall0621_user_info where id in ('${userIdList.mkString("','")}')"
        //        执行查询操作，到Phoenix中的用户表中获取数据
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //        将list集合转换成map，并且将json对象转换成用户样例类对象
        val userMap: Map[String, UserInfo] = userJsonObjList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap

        for (orderInfo <- orderInfoList) {
          val userInfo: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfo != null) {
            orderInfo.user_gender = userInfo.gender_name
            //把生日转成年龄
            val formattor = new SimpleDateFormat("yyyy-MM-dd")
            val date: Date = formattor.parse(userInfo.birthday)
            val curTs: Long = System.currentTimeMillis()
            val betweenMs = curTs - date.getTime
            val age = betweenMs / 1000L / 60L / 60L / 24L / 365L
            if (age < 20) {
              userInfo.age_group = "20岁及以下"
            } else if (age > 30) {
              userInfo.age_group = "30岁以上"
            } else {
              userInfo.age_group = "21岁到30岁"
            }
            if (userInfo.gender == "M") {
              userInfo.gender_name = "男"
            } else {
              userInfo.gender_name = "女"
            }
            orderInfo.user_age_group = userInfo.age_group
          }
        }
        orderInfoList.toIterator
      }
    }
//    orderInfoWithUserInfoDStream.print(1000)


//    保存操作
    import org.apache.phoenix.spark._
    orderInfoWithUserInfoDStream.foreachRDD{
      rdd => {
//        优化：缓存rdd
        rdd.cache()
        val orderInfoFirstRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
//        phoenix提供了对Spark的支持，可以直接将RDD数据保存到Phoenix中
//        如果使用phoenix，要求RDD中存放的类型的属性和Phoenix表中列的数量保持一致
        val firstOrderUserRDD: RDD[UserStatus] = orderInfoFirstRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }

        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS0621",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )

//        将订单数据保存到ES中
        rdd.foreachPartition(
          orderInfoItr => {
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString,orderInfo))
            /*val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall0621_order_info_" + dateStr)*/

//            将订单数据保存到Kafka的dwd_order_info中
            for((orderId,orderInfo) <- orderInfoList){
              MyKafkaSink.send("dwd_order_info",JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }
          }
        )


        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()


  }
}
