package com.atguigu.gmallpubisher.service;

import java.util.Map;

public interface PublisherService {
    Long getDau(String date);

    /**
     * 小时日活
     * Map(hour->"10",  count->1000)
     * Map(hour->"11",  count->20)
     * Map(hour->"12",  count->30)
     * <p>
     * 得到一个->
     * Map("10"->1000, "11"->20, "12"->30)
     *
     * @param date
     * @return
     */
    Map<String, Long> getHourDau(String date);


    Double getTotalAmount(String date);

    //Map("10"->11.11, "11"->20.33, "12"->30.55)
    Map<String, Double> getHourOrderAmount(String date);


    // 销售明细的读取
    /*
    总数, 明细, 聚合的结果
    "total"->100
    "detail" -> Map[f->v, f->v]
    "agg" -> Map[M->10, F->20]
     */
    Map<String, Object> getSaleDetailAndAggGroupByField(String date,
                                                        String keyWord,
                                                        int startPage,
                                                        int sizePerPage,
                                                        int aggField,
                                                        int aggCount);
}
