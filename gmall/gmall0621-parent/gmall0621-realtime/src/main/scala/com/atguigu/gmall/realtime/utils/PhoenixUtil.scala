package com.atguigu.gmall.realtime.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @Description: 从Phoenix中查询数据
 * @Author: msz
 * @UpdateTime: 2020/11/23 6:53
 *
 */
object PhoenixUtil {
  def queryList(sql: String): List[JSONObject] = {
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //    注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //    创建连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    //    创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //    执行sql语句
    val rs: ResultSet = ps.executeQuery()
    //    处理结果集
    val metaData: ResultSetMetaData = rs.getMetaData
    while(rs.next()){

      val jsonObj = new JSONObject()
      for(i <- 1 to metaData.getColumnCount){
        jsonObj.put(metaData.getColumnName(i),rs.getObject(i))
      }
    rsList.append(jsonObj)


    }
    //    释放资源
    rs.close()
    ps.close()
    conn.close()
    rsList.toList
  }

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from user_status0621")
    println(list)
  }
}
