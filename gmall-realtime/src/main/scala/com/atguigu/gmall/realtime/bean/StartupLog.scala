package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String = "",  // 2020-03-30
                      var logHour: String = ""){ // 10 11
    val d = new Date(ts)
    logDate = new SimpleDateFormat("yyyy-MM-dd").format(d)
    logHour = new SimpleDateFormat("HH").format(d)
}

