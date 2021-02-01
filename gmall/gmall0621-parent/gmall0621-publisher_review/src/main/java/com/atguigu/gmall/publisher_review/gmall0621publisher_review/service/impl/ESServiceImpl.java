package com.atguigu.gmall.publisher_review.gmall0621publisher_review.service.impl;

import com.atguigu.gmall.publisher_review.gmall0621publisher_review.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//@Service表示标记，与controller作用一样，将对象的创建交给IOC容器管理
@Service
public class ESServiceImpl implements ESService {
    //    自动注入JestClient
    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query).addIndex("gmall0621_dau_info" + date + "-query").build();
        Long total = 0L;
        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
        return total;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> rsMap = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.aggregation(AggregationBuilders.terms("").field());
        searchSourceBuilder.aggregation(
                new TermsAggregationBuilder("dauHourGroupBy", ValueType.LONG)
                        .field("hr")
                        .size(24)
        );
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex("gmall0621_dau_info" + date + "-query")
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation termsAggregation = result.getAggregations().getTermsAggregation("dauHourGroupBy");
            if(termsAggregation != null){
                List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    rsMap.put(bucket.getKey(),bucket.getCount());

                }
            }
            return rsMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
    }
}
