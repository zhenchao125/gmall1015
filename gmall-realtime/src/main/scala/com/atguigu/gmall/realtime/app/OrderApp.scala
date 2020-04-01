package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.OrderInfo
import com.atguigu.gmall.realtime.util.MykafkaUtil
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/4/1 14:02
 */
object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream: DStream[String] = MykafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
        // 1. 把读取到json字符串进行解析, 封装到样例类中
        val orderInfoStream: DStream[OrderInfo] = sourceStream.map(s => JSON.parseObject(s, classOf[OrderInfo]))
        // 2. 保存到hbase(phoenix)
        orderInfoStream.foreachRDD(rdd => {
            rdd.saveToPhoenix("GMALL_ORDER_INFO1015",
                Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
