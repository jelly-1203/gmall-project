package com.atguigu.gmall.realtime.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

object MySQLUtil {
  def queryList(sql:String):List[JSONObject] = {
    val rsList:ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    Class.forName("com.mysql.jdbc.Driver")
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_realtime_rs?characterEncoding=utf-8&useSSL=false",
      "root",
      "123456"
    )
    val ps: PreparedStatement = conn.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    while(rs.next()){
      val metaData: ResultSetMetaData = rs.getMetaData
      val jsonObj = new JSONObject()
      for (i <- 1 to metaData.getColumnCount){
        jsonObj.put(metaData.getColumnName(i), rs.getObject(i))
      }
      rsList.append(jsonObj)
    }
    rsList.toList
  }
}
