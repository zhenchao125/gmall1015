package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2020/3/28 15:45
 */
/*@Controller
@ResponseBody*/
@RestController   // 等价于 @Controller + @ResponseBody
public class LoggerController {
    //get:  http://localhost:8080/log?log=aa
    //post:  http://localhost:8080/log     参数: 在请求体中

    @PostMapping("/log")
    public String logger(@RequestParam("log") String log) {

        //1. 给数据加时间戳
        log = addTs(log);
        //2. 数据落盘 (比如给离线需求使用, flume可以去采集落盘的文件)
        saveToFile(log);
        //3. 写入到kafka中
        sendToKafka(log);

        return "ok";
    }

    /**
     * 把数据写入到kafka中
     * ----1. 先创建kafka的生产者, 然后调用生产者的send方法
     *
     * @param log
     */
    @Autowired  // 自动注入
    KafkaTemplate template;
    private void sendToKafka(String log) {
        String topic = Constant.TOPIC_STARTUP;
        if(log.contains("event")){
            topic = Constant.TOPIC_EVENT;
        }
        template.send(topic, log);
    }

    /**
     * 日志的落盘, 使用的log4j来完成
     *
     * @param log
     */

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);
    private void saveToFile(String log) {
        logger.info(log);
    }

    /**
     * 给日志添加时间戳
     *
     * @param log 原始日志
     * @return 添加了时间戳的日志
     */
    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return obj.toJSONString();
    }
}
/*
写磁盘, 使用log4j来写.
但是, springboot, 内置的日志框架是logging
所以, 1. 需要把logging去掉, 2. 换成log4j
----
在终端起一个虚拟机:
1. java -jar gmall-logger-0.0.1-SNAPSHOT.jar
2. java -cp gmall-logger-0.0.1-SNAPSHOT.jar org.springframework.boot.loader.JarLauncher


 */