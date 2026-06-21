package com.data.example.dto;

import com.data.example.bean.TaskDefinitionJson;
import com.data.example.bean.TaskRelationJson;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * 功能：创建工作流请求 DTO
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/20
 */
@Data
public class WorkflowRequest {

    /** 工作流名称 */
    private String name;

    /** 工作流描述 */
    private String description;

    /** 任务定义列表 */
    @JsonProperty("taskDefinitionJson")
    private List<TaskDefinitionJson> tasks;

    /** 任务间依赖关系（DAG 的边） */
    @JsonProperty("taskRelationJson")
    private List<TaskRelationJson> taskRelations;

    /** 执行类型: PARALLEL(并行) / SERIAL(串行) */
    private String executionType = "PARALLEL";

    /** 全局参数 JSON 数组字符串 */
    private String globalParams = "[]";

    /** 超时时间（分钟），0表示不超时 */
    private Integer timeout = 0;
}
