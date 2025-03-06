package com.data.example.bean;

import lombok.Data;

/**
 * 功能：项目
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/3/5 16:15
 */
@Data
public class Project {
    private int id;
    private int userId;
    private String userName;
    private long code;
    private String name;
    private String description;
    private String createTime;
    private String updateTime;
    private int perm;
    private int defCount;
    private int instRunningCount;
}
