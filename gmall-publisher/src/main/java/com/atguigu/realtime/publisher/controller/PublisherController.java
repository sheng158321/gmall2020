package com.atguigu.realtime.publisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //集合存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //新增日活及交易额
        int dauTotal = publisherService.getDauTotal(date);
        Double orderAmount = publisherService.getOrderAmount(date);

        //封装新增日活
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //封装新增设备
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //封装新增设备
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", orderAmount);

        //放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {
        //1.创建Map用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //2.获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //3.声明存放昨天和今天数据的Map
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //a.获取当天的日活分时数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //b.获取昨天的日活分时数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        } else if ("order_amount".equals(id)) {
            //a.获取当天的交易额分时数据
            todayMap = publisherService.getOrderAmountHour(date);
            //b.获取昨天天的交易额分时数据
            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }

        //4.将两个Map放入result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //5.返回结果
        return JSONObject.toJSONString(result);
    }
}
