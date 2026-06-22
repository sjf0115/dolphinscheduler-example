package com.data.example.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * 功能：任务定义
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/22
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskDefinition {
    private Integer id;
    private Long code;
    private String name;
    private Integer version;
    private String taskType;
    private Object taskParams;
    private String flag;
    private String description;
    private Integer timeout;
}
