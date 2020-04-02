package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.EventLog
import com.atguigu.gmall.realtime.util.MykafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/4/2 9:07
 */
object AlertApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 1. 先从kafka消费数据
        val sourceStream = MykafkaUtil
            .getKafkaStream(ssc, Constant.TOPIC_EVENT)
            .window(Minutes(5), Seconds(6)) // 添加窗口
        // 2. 数据封装
        val eventLogStream = sourceStream.map(s => JSON.parseObject(s, classOf[EventLog]))
        
        // 3. 实现需求
        // 3.1 按照设备id进行分组
        val eventLogGrouped: DStream[(String, Iterable[EventLog])] = eventLogStream
            .map(eventLog => (eventLog.mid, eventLog))
            .groupByKey
        // 3.2 产生预警信息
        eventLogGrouped.map{
            case (mid, eventLogs)  =>
            
        }
        
        eventLogStream.print(1000)
        
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
并且在登录到领劵过程中没有浏览商品。同时达到以上要求则产生一条预警日志。
同一设备，每分钟只记录一次预警。

1. 先从kafka消费数据

2. 数据封装

3. 实现需求
    1. 同一设备  按照设备id分组
    2. 5分钟内   window的概念: 窗口长度 窗口的滑动步长(6秒中统计一次)
    3. 三次及以上用不同账号登
            统计登录的用户数(聚合)
    4. 领取优惠劵
            过滤
    5. 领劵过程中没有浏览商品
            登录后没有浏览商品
    -------
    6. 同一设备，每分钟只记录一次预警。
            spark-steaming不实现, 交个es自己来实现
                es中, id没分钟变化一次.

4. 把预警信息写入到es

 */