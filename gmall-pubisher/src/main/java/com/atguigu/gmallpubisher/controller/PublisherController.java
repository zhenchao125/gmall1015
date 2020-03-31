package com.atguigu.gmallpubisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpubisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/3/31 10:33
 */
@RestController
public class PublisherController {
    @Autowired
    public PublisherService service;

    // http://localhost:8070/realtime-total?date=2020-03-31
    @GetMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        ArrayList<Map<String, String>> result = new ArrayList<>();

        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", service.getDau(date).toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);


        return JSON.toJSONString(result);
    }
}
/*
[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233} ]

 */