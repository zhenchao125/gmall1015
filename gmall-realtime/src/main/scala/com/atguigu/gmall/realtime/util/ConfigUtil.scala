package com.atguigu.gmall.realtime.util

import java.util.Properties

/**
 * Author atguigu
 * Date 2020/3/30 11:21
 */
object ConfigUtil {
    val is = ConfigUtil.getClass.getClassLoader.getResourceAsStream("config.properties")
    private val props = new Properties()
    props.load(is)
    
    def getProperty(key: String) = props.getProperty(key)
    
}
