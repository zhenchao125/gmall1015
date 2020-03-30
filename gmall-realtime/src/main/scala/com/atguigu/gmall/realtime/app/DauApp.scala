package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/3/30 11:11
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        // 1. 从kafka消费数据
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream = MykafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)
        // 1.1 把数据封装到样例类中.  解析json字符串的时候, 使用fastJson比较方便
        val startupLogStream = sourceStream.map(jsonStr => JSON.parseObject(jsonStr, classOf[StartupLog]))
        
        // 2. 过滤去重得到日活明细
        // 2.1 需要借助第三方的工具进行去重: redis
        val firstStartupLogStream: DStream[StartupLog] = startupLogStream.transform(rdd => {
            // 这些是在driver中
            // 2.2 从redis中读取已经启动的设备
            val client: Jedis = RedisUtil.getClient
            val key: String = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val mids: util.Set[String] = client.smembers(key)
            client.close()
            // 2.3 把已经启动的设备过滤掉.  rdd中只留下那些在redis中不存在的那些记录
            val midsBD: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
            // 2.4 考虑到某个mid在一个批次内启动了多次(而且是这个mid第一次启动), 会出现重复情况
            rdd
                .filter(log => !midsBD.value.contains(log.mid))
                .map(log => (log.mid, log))
                .groupByKey()
                .map {
                    //                    case (_, it) => it.toList.sortBy(_.ts).head
                    case (_, it) => it.toList.minBy(_.ts)
                }
            
        })
        import org.apache.phoenix.spark._
        // 2.4 把第一次启动的设备保存到 redis 中
        firstStartupLogStream.foreachRDD(rdd => {
            rdd.foreachPartition(logIt => {
                // 获取连接
                val client: Jedis = RedisUtil.getClient
                logIt.foreach(log => {
                    // 每次想set中存入一个mid
                    client.sadd(Constant.TOPIC_STARTUP + ":" + log.logDate, log.mid)
                })
                client.close()
            })
            
            // 3. 写到 hbase. 每个mid的每天的启动记录只有一条
            rdd.saveToPhoenix(Constant.DAU_TABLE,
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })
        
        
        firstStartupLogStream.print(1000)
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
redis去重的逻辑:
1. 把已经启动的设备id保存到redis中, 用set集合, 就可以只保留一个
set
key                                         value
"topic_startup:" + 2020-03-30               mid1,mid2
2. 对启动记录过滤, 已经启动过(redis中有记录)的不写到hbase中
    每3秒读一次

 */