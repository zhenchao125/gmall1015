package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/4/7 14:33
 */
object SaleDetailApp1 {
    /**
     * 写数据到redis
     *
     * @param client
     * @param key
     * @param value
     */
    def saveToRedis(client: Jedis, key: String, value: AnyRef): Unit = {
        import org.json4s.DefaultFormats
        val json = Serialization.write(value)(DefaultFormats)
        // 把数据存入到redis
        //        client.set(key, json)
        // 添加了过期时间  超过60*30秒之后这个key会自动删除
        client.setex(key, 60 * 30, json)
    }
    
    /**
     * 缓存OrderInfo
     *
     * @param orderInfo
     * @return
     */
    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo) = {
        val key = "order_info:" + orderInfo.id
        saveToRedis(client, key, orderInfo)
    }
    
    /**
     * 把orderDetail缓存到Redis中
     *
     * @param client
     * @param orderDetail
     * @return
     */
    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
        val key = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
        saveToRedis(client, key, orderDetail)
    }
    
    
    import scala.collection.JavaConversions._
    
    /**
     * 对传入的两个流进行fullJoin
     *
     * @param orderInfoStream
     * @param orderDetailStream
     * @return
     */
    def fullJoin(orderInfoStream: DStream[(String, OrderInfo)],
                 orderDetailStream: DStream[(String, OrderDetail)]): DStream[SaleDetail] = {
        orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it => {
            // 1. 获取redis客户端
            val client: Jedis = RedisUtil.getClient
            
            // 2. 对各种延迟做处理  (如果返回一个就把一个放在集合中, 如果返回的是空, 就返回一个空集合 ...)
            val result = it.flatMap {
                case (orderId, (Some(orderInfo), opt)) =>
                    // 写缓冲
                    cacheOrderInfo(client, orderInfo)
                    // 不管opt是some还是none, 总是要去读OrderDetail的缓冲区
                    val keys: List[String] = client.keys(s"order_detail:${orderId}:*").toList
                    // 3.1 集合中会有多个OrderDetail
                    keys.map(key => {
                        val orderDetailString: String = client.get(key)
                        client.del(key) // 防止这个orderDetail被重复join
                        val orderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
                        
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    }) ::: (opt match {
                        case Some(orderDetail) =>
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                        case None =>
                            Nil
                    })
                // b: oderInfo没有对应的数据, orderDetail存在
                case (orderId, (None, Some(orderDetail))) =>
                    // 1. 根据orderDetail中的orderId去缓存读取对应的orderInfo信息
                    val orderInfoString: String = client.get("order_info:" + orderId)
                    println("None", "some")
                    // 2. 读取之后, 有可能读到对应的OderInfo信息, 也有可能没有读到. 分别处理
                    // 2.1 读到, 把数据封装SaleDetail中去
                    if (orderInfoString != null && orderInfoString.nonEmpty) {
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                    } else { // 2.2 读不到, 把OrderDetail写到缓存
                        cacheOrderDetail(client, orderDetail)
                        Nil
                    }
                
            }
            // 3. 关闭客户端
            client.close()
            
            // 4. 返回处理后的结果
            result
        })
    }
    
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
                (orderInfo.order_id, orderInfo) // order_id就是和order_info表进行管理的条件
            })
        // 3. 双流join
        val saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)
        saleDetailStream.print(1000)
        // 4. 根据用户的id反查mysql中的user_info表, 得到用户的生日和性别
        
        
        // 5. 把详情写到es中
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
redis
redis写到什么样的数据类型

hash?
    key                              value(hash)
    "order_info"                     field                              value
                                     order_id                           整个order_info的所有数据(json字符串)
    
    "order_detail"                   order_id:order_detail_id           整个order_detail的所有数据(json字符串)
    
-----

String(json) ?
    
    key                                                     value(字符串)
    "order_info:" + order_id                                整个order_info的所有数据(json字符串)
    
    "order_detail:" + order_id + ":" + order_detail_id      整个order_detail的所有数据(json字符串)
    




 */