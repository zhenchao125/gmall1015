package com.atguigu.gmall.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString

import scala.collection.JavaConversions._

/**
 * Author atguigu
 * Date 2020/4/1 9:14
 */
object CanalClient {
    def main(args: Array[String]): Unit = {
        // 1. 连接到canal
        val address = new InetSocketAddress("hadoop102", 11111)
        val connector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
        connector.connect() // 连接
        // 1.1 订阅数据  gmall1015.* 表示gmall1015数据下所有的表
        connector.subscribe("gmall1015.*")
        // 2. 读数据, 解析数据
        while (true) { // 2.1 使用循环的方式持续的从canal服务中读取数据
            val msg: Message = connector.get(100) // 2.2 一次从canal拉取最多100条sql数据引起的变化
            // 2.3 一个entry封装一条sql的变化结果   . 做非空的判断
            val entriesOption: Option[util.List[CanalEntry.Entry]] = if (msg != null) Some(msg.getEntries) else None
            if (entriesOption.isDefined) {
                val entries: util.List[CanalEntry.Entry] = entriesOption.get
                for(entry <- entries){
                    // 2.4 从每个entry中获取一个StoreValue
                    val storeValue: ByteString = entry.getStoreValue
                    // 2.5 把storeValue解析出来rowChange
                    val rowChange: RowChange = RowChange.parseFrom(storeValue)
                    // 2.6 一个storeValue中有多个RowData, 每个RowData表示一行数据的变化
                    val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
                    // 2.7 解析rowDatas中的每行的每列的数据
                    
                }
            }
            
            
        }
        
        // 3. 把数据转成json字符串写入到kafka中.  {列名: 列值, 列名: 列值,....}
    }
}
