package com.data.example.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.Date;

/**
 * 功能：工作流定义
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:27
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessDefinition {
    private Long code;
    private String name;
    private Long projectCode;
    private String releaseState; // ONLINE / OFFLINE
    private String description;
    private Date createTime;
    private Date updateTime;
}
