package com.atguigu.gmall.realtime.utils

import com.atguigu.gmall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyESUtil {


  var factor: JestClientFactory = null

  def getJestClient: JestClient = {
    if (factor == null) {
      build()
    }
    factor.getObject
  }

  def build(): Unit = {
    factor = new JestClientFactory
    factor.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000)
      .multiThreaded(true)
      .build())
  }

  def bulkInsert(sourceList: List[(String,Any)], indexName: String): Unit = {
    if (sourceList != null && sourceList.size > 0) {
      val jestClient: JestClient = getJestClient
      val builder = new Bulk.Builder()
      for ((id,source) <- sourceList) {
        val index: Index = new Index.Builder(source)
          .index(indexName)
          .`type`("_doc")
          .id(id)
          .build()
        builder.addAction(index)
      }
      val bulk: Bulk = builder.build()
      val result: BulkResult = jestClient.execute(bulk)
      println(s"向ES中插入${result.getItems.size()}条")
      jestClient.close()
    }
  }

}
