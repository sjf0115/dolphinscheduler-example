package com.data.example.bean;

import lombok.Builder;
import lombok.Data;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:27
 */
@Data
@Builder
public class TaskDefinitionJson {
    private Long code;
    private Long version;
    private String name;
    private String taskType; // SHELL / SQL / HIVE / SPARK / HTTP 等
    private String taskParams; // JSON 字符串
    private String flag; // YES / NO
    private String description;
    private Integer timeout;
    private String timeoutNotifyStrategy;
}
