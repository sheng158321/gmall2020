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

        //新增日活
        int dauTotal = publisherService.getDauTotal(date);

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

        //放入集合
        result.add(dauMap);
        result.add(newMidMap);


        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {
        //获取当天日活分时数据
        Map todayMap = publisherService.getDauTotalHourMap(date);

        //获取昨天日活分时数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

        //存放结果
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);

    }
}
