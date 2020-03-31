package com.atguigu.gmallpubisher.service;

import com.atguigu.gmallpubisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author lzc
 * @Date 2020/3/31 10:29
 */
@Service
public class PublisherServiceImp implements PublisherService{
    @Autowired
    DauMapper dauMapper;
    @Override
    public Long getDau(String date) {
        return dauMapper.getDau(date);
    }
}

