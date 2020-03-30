package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.util.MykafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
        sourceStream.print(10000)
        
        // 2. 过滤得到日活明细
        
        
        // 3. 写到 hbase
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}
