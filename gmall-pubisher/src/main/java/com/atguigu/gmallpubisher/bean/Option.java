package com.atguigu.gmallpubisher.bean;

/**
 * @Author lzc
 * @Date 2020/4/8 15:38
 */
public class Option {
    private String name;
    private Long value;

    public Option() {
    }

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
