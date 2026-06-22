package com.data.example.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.Date;

/**
 * 功能：定时调度
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/21
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Schedule {
    private Integer id;
    private Long processDefinitionCode;
    private String processDefinitionName;
    private String schedule;
    private String releaseState; // ONLINE / OFFLINE
    private Date createTime;
    private Date updateTime;
}
