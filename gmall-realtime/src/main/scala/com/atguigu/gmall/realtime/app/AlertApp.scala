package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.gmall.realtime.util.MykafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * Author atguigu
 * Date 2020/4/2 9:07
 */
object AlertApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("AlertApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        // 1. 先从kafka消费数据
        val sourceStream: DStream[String] = MykafkaUtil
            .getKafkaStream(ssc, Constant.TOPIC_EVENT)
            .window(Minutes(5), Seconds(6)) // 添加窗口
        // 2. 数据封装
        val eventLogStream: DStream[EventLog] = sourceStream
            .map(s => JSON.parseObject(s, classOf[EventLog]))
        
        // 3. 实现需求
        // 3.1 按照设备id进行分组
        val eventLogGrouped: DStream[(String, Iterable[EventLog])] = eventLogStream
            .map(eventLog => (eventLog.mid, eventLog))
            .groupByKey
        // 3.2 产生预警信息
        val alertInfoStream: DStream[(Boolean, AlertInfo)] = eventLogGrouped.map {
            /*
            3. 三次及以上用不同账号登
                    统计登录的用户数(聚合)
            4. 领取优惠劵
                    过滤
            5. 领劵过程中没有浏览商品
                    登录后没有浏览商品
             */
            case (mid, eventLogs: Iterable[EventLog]) =>
                // 注意: 集合需要java的集合, 把集合中的数据写入到es时候, scala的集合不支持(取不出集合中的数据)
                // a: 存储登录的过的所有用户, 统计的在当前设备(mid), 最近5分钟登录过的所有用户
                val uidSet: util.HashSet[String] = new util.HashSet[String]()
                // b: 存储5分钟内当前设备所有的事件
                val eventList: util.ArrayList[String] = new util.ArrayList[String]()
                // c: 存储领取的优惠券对应的那些产品的id
                val itemSet: util.HashSet[String] = new util.HashSet[String]()
                
                // d: 是否点击了商品, 默认是没有点击
                var isClickItem: Boolean = false
                
                breakable {
                    eventLogs.foreach(log => {
                        eventList.add(log.eventId) // 存储所有的事件
                        log.eventId match {
                            case "coupon" =>
                                uidSet.add(log.uid) // 存储领取优惠券的用户id
                                itemSet.add(log.itemId) // 优惠券对应的商品id存储
                            case "clickItem" => // 表示这次事件是浏览商品
                                // 只要有一次浏览商品, 就不应该产生预警信息
                                isClickItem = true
                                break
                            case _ => // 其他事件忽略
                        }
                    })
                }
                //(Boolean, 预警信息)
                //(是否需要预警, 预警信息)
                (uidSet.size() >= 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
        }
        
        // 4. 把预警信息写入到es
        alertInfoStream
            .filter(_._1) // 先把需要写入到es的预警信息过滤出来
            .map(_._2) // 只保留预警信息
            .foreachRDD(rdd => {
                rdd.foreachPartition(alterInfos => {
                    // 连接到es
                    // 写数据
                    // 关闭到es的连接
                })
            })
        
        
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