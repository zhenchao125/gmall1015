package com.atguigu.gmallpubisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/4/8 15:27
 */
public class SaleInfo {
    private Integer total;
    private List<Map<String, Object>> detail;
    private List<Stat> stats = new ArrayList<>();

    /**
     * 直接向集合中添加元素
     * @param stat
     */
    public void addStat(Stat stat){
        stats.add(stat);
    }


    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<Map<String, Object>> getDetail() {
        return detail;
    }

    public void setDetail(List<Map<String, Object>> detail) {
        this.detail = detail;
    }

    public List<Stat> getStats() {
        return stats;
    }
}
