package com.atguigu.gmall.publisher_review.gmall0621publisher_review.service;

import java.util.Map;

public interface ESService {
//    获取某天日活总数
    public Long getDauTotal(String date);
//    获取某天的分时日活数
    public Map<String,Long> getDauHour(String date);
}
