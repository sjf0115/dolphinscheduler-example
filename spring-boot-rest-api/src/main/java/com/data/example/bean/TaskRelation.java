package com.data.example.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * 功能：任务关系（工作流 DAG 中的边）
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/22
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRelation {
    private Integer id;
    private String name;
    private Integer processDefinitionVersion;
    private Long projectCode;
    private Long processDefinitionCode;
    private Long preTaskCode;
    private Integer preTaskVersion;
    private Long postTaskCode;
    private Integer postTaskVersion;
    private String conditionType;
}
