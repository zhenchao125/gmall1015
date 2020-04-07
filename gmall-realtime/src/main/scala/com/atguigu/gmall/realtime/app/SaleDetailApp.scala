package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall.realtime.util.MykafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/4/7 14:33
 */
object SaleDetailApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 1. 读取kafka中的两个topic, 得到两个流
        // 2. 对他们做封装  (join必须是kv形式的, k其实就是他们join的条件)
        val orderInfoStream: DStream[(String, OrderInfo)] = MykafkaUtil
            .getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
            .map(s => {
                val orderInfo = JSON.parseObject(s, classOf[OrderInfo])
                (orderInfo.id, orderInfo)
            })
        val orderDetailStream: DStream[(String, OrderDetail)] = MykafkaUtil
            .getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
            .map(s => {
                val orderInfo = JSON.parseObject(s, classOf[OrderDetail])
                (orderInfo.order_id, orderInfo)  // order_id就是和order_info表进行管理的条件
            })
        orderInfoStream.print(1000)
        orderDetailStream.print(1000)
        
        // 3. 双流join
        
        
        // 4. 根据用户的id反查mysql中的user_info表, 得到用户的生日和性别
        
        
        // 5. 把详情写到es中
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}
