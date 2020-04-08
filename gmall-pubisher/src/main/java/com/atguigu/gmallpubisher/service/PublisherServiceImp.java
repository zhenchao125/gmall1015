package com.atguigu.gmallpubisher.service;

import com.atguigu.gmallpubisher.mapper.DauMapper;
import com.atguigu.gmallpubisher.mapper.OrderInfoMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/3/31 10:29
 */
@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dauMapper;
    @Autowired
    OrderInfoMapper orderInfoMapper;

    @Override
    public Long getDau(String date) {
        return dauMapper.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dauMapper.getHourDau(date);

        HashMap<String, Long> result = new HashMap<String, Long>();

        for (Map<String, Object> map : hourDauList) {
            String hour = (String) map.get("HOUR");  // 10点
            Long count = (Long) map.get("COUNT");  // 10的日活
            result.put(hour, count);
        }
        return result;
    }

    /**
     * 总的销售额
     *
     * @param date
     * @return
     */
    @Override
    public Double getTotalAmount(String date) {
        Double result = orderInfoMapper.getTotalAmount(date);
        return result == null ? 0 : result;
    }

    @Override
    public Map<String, Double> getHourOrderAmount(String date) {
        Map<String, Double> result = new HashMap<>();
        List<Map<String, Object>> hourAmountList = orderInfoMapper.getHourAmount(date);
        for (Map<String, Object> map : hourAmountList) {
            String hour = (String) map.get("CREATE_HOUR");
            Double amount = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(hour, amount);
        }
        return result;
    }

    /**
     * 根据参数从es获取数据
     * @param date
     * @param keyWord
     * @param startPage
     * @param sizePerPage
     * @param aggField
     * @param aggCount
     * @return
     */
    @Override
    public Map<String, Object> getSaleDetailAndAggGroupByField(String date, String keyWord, int startPage, int sizePerPage, int aggField, int aggCount) {
        return null;
    }
}

