package com.atguigu.gmallpubisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    /**
     * 获取日活的接口
     * @param date
     * @return
     */
    Long getDau(String date);

    /**
     * 获取的小时日活
     *
     * * 小时日活
     *      * Map(hour->"10",  count->1000)
     *      * Map(hour->"11",  count->20)
     *      * Map(hour->"12",  count->30)
     * @param date
     * @return
     */
    List<Map<String, Object>> getHourDau(String date);
}
