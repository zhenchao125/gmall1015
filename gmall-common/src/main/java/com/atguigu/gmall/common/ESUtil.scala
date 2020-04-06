package com.atguigu.gmall.common

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
 * Author atguigu
 * Date 2020/4/6 16:10
 */
object ESUtil {
    // 端口号, 服务器端如果没有配置, 默认是9200
    // 9300:  是节点内部通讯的时候使用的
    val esUrl = "http://hadoop102:8300"
    
    // 1. 先有es的客户端
    // 1.1 创建一个客户端工厂
    val factory = new JestClientFactory
    val conf = new HttpClientConfig.Builder(esUrl)
        .maxTotalConnection(100) // 最多同时可以有100个到es的连接的客户端
        .connTimeout(10 * 1000) // 连接到es的超时时间
        .readTimeout(10 * 1000) // 读取数据的的时候的超时时间
        .multiThreaded(true)
        .build()
    factory.setHttpClientConfig(conf)
    
    
    /**
     * 向es中插入单条数据
     *
     * @param index index
     * @param source    数据源
     * @param id    单条数据的id, 如果是null, id会随机生成
     */
    def insertSingle(index: String, source: Object, id: String = null) = {
        // 向es写入数据
        val client: JestClient = factory.getObject
        val action: Index = new Index.Builder(source)
            .index(index)
            .`type`("_doc")
            .id(id)  // id如果是null, 就相当于没有传
            .build()
        client.execute(action)
        client.shutdownClient()
    }
    
    /**
     * 向es中插入批次数据
     * @param index
     * @param sources
     */
    def insertBulk(index: String, sources: Iterator[Any]) = {
        val client: JestClient = factory.getObject
    
        val bulk = new Bulk.Builder()
            .defaultIndex(index)  // 多个doc应该进入同一个index中
            .defaultType("_doc")
        
        sources.foreach(source => {
            val action = new Index.Builder(source).build()
            bulk.addAction(action)
        })
        client.execute(bulk.build())
        client.shutdownClient();
    }
    
    /*def insertBulk() = {
        val user1 = User(100, "hanwudi")
        val action1 = new Index.Builder(user1).build()
        
        val user2 = User(200, "qinshihuang")
        val action2 = new Index.Builder(user2).build()
    
        val client: JestClient = factory.getObject
        // action中应该包含多个doc
        val bulk = new Bulk.Builder()
            .defaultIndex("user")  // 多个doc应该进入同一个index中
            .defaultType("_doc")
            // 添加多条记录
            .addAction(action1)
            .addAction(action2)
            .build()
        client.execute(bulk)
        client.shutdownClient();
    }
    */
    def main(args: Array[String]): Unit = {
        val list = User(1, "a")::User(2, "b")::User(3, "c")::Nil
        insertBulk("user", list.toIterator)
        
        
        
        
        //测试单次插入
        /*val data =
            """
              |{
              |  "name": "zhiling",
              |  "age": 40
              |}
              |""".stripMargin
        insertSingle("user",data)*/
        
        
        
        
        
       /* // 向es写入数据
        
        // 1. 先有es的客户端
        // 1.1 创建一个客户端工厂
        val factory = new JestClientFactory
        val conf = new HttpClientConfig.Builder(esUrl)
            .maxTotalConnection(100) // 最多同时可以有100个到es的连接的客户端
            .connTimeout(10 * 1000) // 连接到es的超时时间
            .readTimeout(10 * 1000) // 读取数据的的时候的超时时间
            .multiThreaded(true)
            .build()
        factory.setHttpClientConfig(conf)
        // 1.2 从工厂获取一个客户端
        val client: JestClient = factory.getObject
        // 2. es需要的数据(json, 样例类)
        /* val data =
             """
               |{
               |  "name": "zs",
               |  "age": 20
               |}
               |""".stripMargin*/
        val data = User(30, "ww")
        // 3. 写入(单次, 批次)
        val index: Index = new Index.Builder(data)
            .index("user")
            .`type`("_doc")
            //            .id("1")  // 可选. 如果没有设置, 则id会自动生成
            .build()
        client.execute(index)
        
        // 4. 关闭客户端(其实把客户端还给工厂)
        client.shutdownClient()*/
    }
}

case class User(age: Int, name: String)