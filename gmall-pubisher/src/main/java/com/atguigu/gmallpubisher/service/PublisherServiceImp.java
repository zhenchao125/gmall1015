package com.atguigu.gmallpubisher.service;

import com.atguigu.gmallpubisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/3/31 10:29
 */
@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDau(String date) {
        return dauMapper.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dauMapper.getHourDau(date);

        HashMap<String, Long> result = new HashMap<String, Long>();

        for (Map<String, Object> map : hourDauList) {
            String hour = (String) map.get("HOUR");  // 10点
            Long count = (Long) map.get("COUNT");  // 10的日活
            result.put(hour, count);
        }
        return result;
    }
}

