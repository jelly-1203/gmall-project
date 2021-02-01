package com.atguigu.gmall.publisher_review.gmall0621publisher_review.controller;

import com.atguigu.gmall.publisher_review.gmall0621publisher_review.service.ESService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
//            本身是接口，但是会找他的实现类
    ESService esService;

/*
* 请求路径：http://publisher:8070/realtime-total?date=2019-02-01
* 返回内容： [
            {"id":"dau","name":"新增日活","value":1200},
            {"id":"new_mid","name":"新增设备","value":233}
        ]
* */
//底层会自动将对象转移成对应格式的json字符串
    @RequestMapping("/realtime-total")
  /*  public Object realtimeTotal(@RequestParam("date") String dt)
    如果有多个参数的时候，使用@RequestParam
    RequestParam 表示要接收哪个参数，将参数赋值给后面的变量，相当于改名字*/
    public Object realtimeTotal(String date){
        List<Map<String,Object>> rsList = new ArrayList<>();
        Map<String,Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",esService.getDauTotal(date));
        rsList.add(dauMap);

        Map<String,Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        rsList.add(newMidMap);

        return rsList;
    }

     /*
     分时统计
	请求路径
		http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
	返回
		{
			"yesterday":{"11":383,"12":123,"17":88,"19":200 },
			"today":{"12":38,"13":1233,"17":123,"19":688 }
		}
     */
     @RequestMapping("/realtime-hour")
     public Map<String,Map<String,Long>> realtimeHour(String date,String id){
         Map<String,Map<String,Long>> rsMap = new HashMap<>();
//         获取今天分时日活
         Map<String,Long> tdMap = new HashMap<>();
         rsMap.put("today",esService.getDauHour(date));
//         获取昨天分时日活
         String yd = getYd(date);
         rsMap.put("yesterday",esService.getDauHour(yd));

         return rsMap;
    }
    //获取昨天日期
    private  String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }

}
