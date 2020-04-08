package com.atguigu.gmallpubisher.service;

import com.atguigu.gmall.common.Constant;
import com.atguigu.gmall.common.ESUtil;
import com.atguigu.gmallpubisher.mapper.DauMapper;
import com.atguigu.gmallpubisher.mapper.OrderInfoMapper;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
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
     * <p>
     * 总数, 明细, 聚合的结果
     * "total"->100
     * "detail" -> Map[f->v, m->v]
     * "agg" -> Map[M->10, F->20]
     *
     * @param date
     * @param keyWord
     * @param startPage
     * @param sizePerPage
     * @param aggField
     * @param aggCount
     * @return
     */
    @Override
    public Map<String, Object> getSaleDetailAndAggGroupByField(String date,
                                                               String keyWord,
                                                               int startPage,
                                                               int sizePerPage,
                                                               String aggField,
                                                               int aggCount) throws IOException {
        Map<String, Object> result = new HashMap<>();

        // 1. 获取es客户端
        JestClient client = ESUtil.getClient();
        // 2. 查询数据(聚合)
        String dsl = DSLs.getSaleDetailDSL(date, keyWord, startPage, sizePerPage, aggField, aggCount);
        Search search = new Search.Builder(dsl)
                .addIndex(Constant.INDEX_SALE_DETAIL)
                .addType("_doc")
                .build();
        // 2.2 es返回的所有数据都封装到了这个对象中
        SearchResult searchResult = client.execute(search);
        // 3. 从返回的结果中, 解析出来我们需要数据
        // 3.1 总数
        Integer total = searchResult.getTotal();
        result.put("total", total);
        // 3.2 明细
        List<HashMap> detail = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            detail.add(source);
        }
        result.put("detail", detail);

        // 3.3 聚合结果
        Map<String, Long> aggMap = new HashMap<>();

        List<TermsAggregation.Entry> buckets = searchResult
                .getAggregations()
                .getTermsAggregation("group_by_" + aggField)
                .getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long count = bucket.getCount();
            aggMap.put(key, count);
        }
        result.put("agg", aggMap);
        // 4. 返回最终的结果  Map, 包含3个k-v
        return result;
    }
}

