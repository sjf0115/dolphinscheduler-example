package com.data.example.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

/**
 * 功能：工作流详情（DS workflowDetail 接口返回的 data 结构）
 *       DS 返回的 data 包含 processDefinition + 关联的 task 和 relation
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/22
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessDefinitionDetail {

    /** 工作流定义主体 */
    private ProcessDefinition processDefinition;

    /** 任务关系列表 */
    private List<TaskRelation> processTaskRelationList;

    /** 任务定义列表 */
    private List<TaskDefinition> taskDefinitionList;
}
