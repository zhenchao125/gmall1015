package com.atguigu.gmalllogger.controller;

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
    //post:  http://localhost:8080/log     参数: 在请求体重

    @PostMapping("/log")
    public String logger(@RequestParam("log") String log){

        // 1. 给数据加时间戳

        //2. 数据落盘 (比如给离线需求使用, flume可以去采集落盘的文件)

        //3. 写入到kafka中

        return "ok";
    }
}
