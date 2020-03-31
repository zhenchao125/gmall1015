package com.atguigu.gmallpubisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmallpubisher.mapper")
public class GmallPubisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPubisherApplication.class, args);
    }

}
