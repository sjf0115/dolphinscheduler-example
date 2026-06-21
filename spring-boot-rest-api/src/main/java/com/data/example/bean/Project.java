package com.data.example.bean;

import lombok.Data;

import java.util.Date;

/**
 * 功能：项目
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:26
 */
@Data
public class Project {
    private Long id;
    private Long code;
    private String name;
    private String description;
    private String userName;
    private Date createTime;
    private Date updateTime;
}
