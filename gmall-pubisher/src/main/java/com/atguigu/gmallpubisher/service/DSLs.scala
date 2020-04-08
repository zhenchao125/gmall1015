package com.atguigu.gmallpubisher.service

/**
 * Author atguigu
 * Date 2020/4/8 14:20
 */
object DSLs {
    def getSaleDetailDSL(date: String,
                        keyWord: String,
                        startPage: Int,
                        sizePerPage: Int,
                        aggField: String,
                        aggCount: Int) = {
        s"""
          |{
          |  "query": {
          |    "bool": {
          |      "filter": {
          |        "term": {
          |          "dt": "${date}"
          |        }
          |      },
          |      "must": [
          |        {"match": {
          |          "sku_name": {
          |            "query": "${keyWord}",
          |            "operator": "and"
          |          }
          |        }}
          |      ]
          |    }
          |  },
          |  "aggs": {
          |    "group_by_${aggField}": {
          |      "terms": {
          |        "field": "${aggField}",
          |        "size": ${aggCount}
          |      }
          |    }
          |  },
          |  "from": ${(startPage - 1) * sizePerPage},
          |  "size": ${sizePerPage}
          |}
          |""".stripMargin
    }
}
