package com.atguigu.realtime.publisher.service.impl;

import com.atguigu.realtime.publisher.mapper.DauMapper;
import com.atguigu.realtime.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

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
}
