package com.atguigu.gmall.realtime.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.{Constant, ESUtil}
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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
    
    /**
     * 使用spark-sql去读mysql中的数据, 然后把需要的字段添加到 SaleDetail
     *
     * @param saleDetailStream
     * @param ssc
     */
    def joinUser(saleDetailStream: DStream[SaleDetail], ssc: StreamingContext) = {
        val url = "jdbc:mysql://hadoop102:3306/gmall1015"
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaaaaa")
        val spark = SparkSession.builder()
            .config(ssc.sparkContext.getConf) // 给sparkSession进行配置的时候, 使用ssc.sparkContext的配置
            .getOrCreate()
        import spark.implicits._
        // 1. 先把mysql数据读进来 每隔3s读一次
        saleDetailStream.transform((SaleDetailRDD: RDD[SaleDetail]) => {
            /*
                2. 读mysql数据的时候, 有两种读法:
                  2.1 直接会用rdd的join完成
                  2.2 每个分区自己负责join(map端join)  参考map端的join代码
             */
            // 2.1 直接会用rdd的join完成
            val userInfoRDD: RDD[(String, UserInfo)] = spark
                .read
                .jdbc(url, "user_info", props)
                .as[UserInfo]
                .rdd
                .map(user => (user.id, user))
    
            // 2.2 两个RDD做join
            SaleDetailRDD
                .map(saleDetail => (saleDetail.user_id, saleDetail))
                .join(userInfoRDD)
                .map {
                    case (_, (saleDetail, userInfo)) =>
                        saleDetail.mergeUserInfo(userInfo)
            
                }
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
        var saleDetailStream: DStream[SaleDetail] = fullJoin(orderInfoStream, orderDetailStream)
        
        // 4. 根据用户的id反查mysql中的user_info表, 得到用户的生日和性别
        saleDetailStream = joinUser(saleDetailStream, ssc)
        // 5. 把详情写到es中
        saleDetailStream.foreachRDD(rdd => {
            // 方法1: 可以把rdd的所有数据拉倒驱动端, 一次性写入
            ESUtil.insertBulk("sale_detail_1015", rdd.collect().toIterator)
            // 方法2: 每个分区分分别去写
            /*rdd.foreachPartition((it: Iterator[SaleDetail]) => {
                ESUtil.insertBulk("sale_detail_1015", it)
            })*/
        })
        
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