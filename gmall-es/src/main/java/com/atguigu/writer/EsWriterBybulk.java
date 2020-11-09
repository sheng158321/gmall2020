package com.atguigu.writer;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterBybulk {
    public static void main(String[] args) throws IOException {
        //创建
        JestClientFactory jestClientFactory = new JestClientFactory();
        //设置来连接属性
        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(config);
        //获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        //准备数据
        Movie movie1 = new Movie("1004", "他");
        Movie movie2 = new Movie("1005", "她");
        Index index1 = new Index.Builder(movie1).id("1004").build();
        Index index2 = new Index.Builder(movie2).id("1005").build();
        Bulk build = new Bulk.Builder()
                .defaultIndex("movie_test2")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();
        //写入数据
        jestClient.execute(build);
        //关闭客户端
        jestClient.shutdownClient();
    }
}
