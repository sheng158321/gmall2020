package com.atguigu.realtime.publisher.service;

import java.util.Map;

public interface PublisherService {
    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);
}