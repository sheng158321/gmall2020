package com.atguigu.realtime.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.realtime.publisher.mapper")
public class PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublisherApplication.class, args);
    }

}


/**
 * 外部请求去controller
 * controller调用service
 * service调用dau层，即mapper
 *
 * application.properties配置中框架可以扫描mapper下所有xml文件
 *@MapperScan(basePackages = "com.atguigu.realtime.publisher.mapper")框架可以扫描mapper所有类
 * */