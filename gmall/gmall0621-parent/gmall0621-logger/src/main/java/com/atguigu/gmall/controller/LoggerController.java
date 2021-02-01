package com.atguigu.gmall.controller;

// 接收日志模拟器生成的日志数据，并对其进行处理,给客户端响应

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;


@Controller
@RestController
@Slf4j
public class LoggerController {
    //   SpringBoot继承了对Kafka的处理类
//        ！！！注入：将SpringBoot继承了对Kafka的处理类注入到Controller中，给属性赋了值，如果不注入，则就是普通属性！！！！
    @Autowired
    KafkaTemplate kafkaTemplate;


    //    接收指定的请求，将请求交给标记的方法进行处理
    @RequestMapping("/applog")
//    会将返回的字符串，当做json进行响应

    @ResponseBody
    public String  applog(@RequestBody String jsonLog){
//        System.out.println(jsonLog);
        log.info(jsonLog);

   /*     new KafkaProducer<String,String>(new Properties())
                .send(new ProducerRecord<String,String>(
                        "topicA","xxxx"
                ));*/

        JSONObject jsonObject = JSON.parseObject(jsonLog);
        if(jsonObject.getJSONObject("start") != null){
            kafkaTemplate.send("gmall_start_0621",jsonLog);

        }else{
            kafkaTemplate.send("gmall_event_0621",jsonLog);

        }

        return "success";
    }
}
