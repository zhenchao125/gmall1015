package com.atguigu.gmallpubisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/4/1 14:58
 */
public interface OrderInfoMapper {
    // 1. 总的销售额
    Double getTotalAmount(String date);

    // 2. 小时的销售额
    /*
     * 小时日活
     *      * Map(hour->"10",  sum->2000.11)
     *      * Map(hour->"11",  sum->234.22)
     *      * Map(hour->"12",  sum->22.55)
     */
    List<Map<String, Object>> getHourAmount(String date);
}
