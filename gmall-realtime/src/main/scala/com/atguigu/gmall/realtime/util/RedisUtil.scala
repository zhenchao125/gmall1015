package com.atguigu.gmall.realtime.util

import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/3/30 14:17
 */
object RedisUtil {
    val host: String = ConfigUtil.getProperty("redis.host")
    val port: Int = ConfigUtil.getProperty("redis.port").toInt
    
    def getClient: Jedis = {
        val client: Jedis = new Jedis(host, port, 60 * 1000)
        client.connect()
        client
    }
}
