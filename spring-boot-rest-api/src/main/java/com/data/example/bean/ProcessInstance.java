package com.data.example.bean;

import lombok.Data;

import java.util.Date;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:27
 */
@Data
public class ProcessInstance {
    private Integer id;
    private String name;
    private Long processDefinitionCode;
    private String state; // SUCCESS / FAILURE / RUNNING / STOP
    private Date startTime;
    private Date endTime;
    private Integer commandType;
}
