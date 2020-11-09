package com.atguigu.realtime.publisher.service.impl;

import com.atguigu.realtime.publisher.mapper.DauMapper;
import com.atguigu.realtime.publisher.mapper.OrderMapper;
import com.atguigu.realtime.publisher.service.PublisherService;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        //查询phoenix获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //存放数据
        HashMap<String, Object> result = new HashMap<>();

        //遍历list
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }

    @Override
    public Double getOrderAmount(String date) {

        return orderMapper.selectOrderAmountTotal(date);

    }

    @Override
    public Map getOrderAmountHour(String date) {

        //获取phoenix数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        //存放数据的map
        HashMap<String, Double> result = new HashMap<>();
        //遍历集合list
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        return result;
    }
}
