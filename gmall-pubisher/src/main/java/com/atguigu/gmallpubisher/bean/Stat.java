package com.atguigu.gmallpubisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/4/8 15:32
 */
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public void addOption(Option option){
        options.add(option);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

}
