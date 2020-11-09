package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.MinAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import sun.util.resources.cldr.ta.CurrencyNames_ta;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class EsReader {
    public static void main(String[] args) throws IOException {
        //创建
        JestClientFactory jestClientFactory = new JestClientFactory();
        //设置来连接属性
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);
        //获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        //查询数据
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "0621");
        boolQueryBuilder.filter(termQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合组
        MinAggregationBuilder minAgeGroup = AggregationBuilders.min("minAge").field("age");
        TermsAggregationBuilder countByGenderGroup = AggregationBuilders.terms("countByGender").field("gender");
        searchSourceBuilder.aggregation(minAgeGroup);
        searchSourceBuilder.aggregation(countByGenderGroup);

        //分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student2")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(search);
        //解析数据
        //总数
        Long total = result.getTotal();
        System.out.println("总命中：" + total + "条数据！");
        //数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            System.out.println("*************************");
            for (Object o : source.keySet()) {
                System.out.println("Key:" + o + ",Value:" + source.get(o));
            }
        }
        //解析聚合组
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation countByGender = aggregations.getTermsAggregation("countByGender");
        for (TermsAggregation.Entry bucket : countByGender.getBuckets()) {
            System.out.println(bucket.getKeyAsString() + ":" + bucket.getCount());
        }
        MinAggregation minAge = aggregations.getMinAggregation("minAge");
        System.out.println(minAge.getMin());

        //关闭客户端
        jestClient.shutdownClient();
    }
}
