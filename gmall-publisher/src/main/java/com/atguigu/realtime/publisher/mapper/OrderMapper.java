package com.atguigu.realtime.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    public Double selectOrderAmountTotal(String date);
    public List<Map> selectOrderAmountHourMap(String date);
}
