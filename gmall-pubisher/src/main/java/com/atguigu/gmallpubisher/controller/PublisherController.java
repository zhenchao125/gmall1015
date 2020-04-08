package com.atguigu.gmallpubisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpubisher.bean.SaleInfo;
import com.atguigu.gmallpubisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        System.out.println(service.getTotalAmount(date));
        map3.put("value", service.getTotalAmount(date).toString());
        result.add(map3);


        return JSON.toJSONString(result);
    }

    // http://localhost:8070/realtime-hour?id=dau&date=2020-03-31
    @GetMapping("/realtime-hour")
    public String realtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals(id)) {

            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);

            return JSON.toJSONString(result);
        } else if ("order_amount".equals(id)) {
            Map<String, Double> today = service.getHourOrderAmount(date);
            Map<String, Double> yesterday = service.getHourOrderAmount(getYesterday(date));

            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);

            return JSON.toJSONString(result);
        }
        return "";
    }

    /**
     * 根据今天计算出来昨天
     * "2020-03-31" -> "2020-03-30"
     *
     * @param today
     * @return
     */
    private String getYesterday(String today) {
//        return LocalDate.parse(today).plusDays(-1).toString();
        return LocalDate.parse(today).minusDays(1).toString();
    }


    /**
     * saleDetail的接口
     * http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
     */
    @GetMapping("/sale_detail")
    public String SaleDetail(@RequestParam("date") String date,
                             @RequestParam("startpage") int startpage,
                             @RequestParam("size") int size,
                             @RequestParam("keyword") String keyword) throws IOException {

        Map<String, Object> resultGender = service.getSaleDetailAndAggGroupByField(
                date,
                keyword,
                startpage,
                size,
                "user_gender",
                2);
        Map<String, Object> resultAge = service.getSaleDetailAndAggGroupByField(
                date,
                keyword,
                startpage,
                size,
                "user_age",
                100);

        // 0. 最终的结果
        SaleInfo saleInfo = new SaleInfo();
        // 1. 封装总数(聚合结果中, 任何一个都可以)
        saleInfo.setTotal((Integer)resultAge.get("total"));
        // 2. 封装明细
        List<Map<String, Object>> detail = (List<Map<String, Object>>) resultAge.get("detail");
        saleInfo.setDetail(detail);
        // 3. 封装饼图(Stat)

        return JSON.toJSONString(saleInfo);
    }

}
/*
[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233} ]


{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}

---------

[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233 },
{"id":"order_amount","name":"新增交易额","value":1000.2 }]

 */