package com.data.example.service;

import com.data.example.bean.*;
import com.data.example.client.DolphinSchedulerApiClient;
import com.data.example.dto.WorkflowRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.data.example.utils.CommonUtil.toJson;

/**
 * 功能：工作流业务 Service
 *       封装业务逻辑：参数转换、校验、编排、错误处理
 *       Controller 只负责 HTTP 映射，Service 负责业务逻辑
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/20
 */
@Slf4j
@Service
public class WorkflowService {
    @Autowired
    private DolphinSchedulerApiClient client;

    /**
     * 创建工作流
     * 职责：DTO → Map 参数转换 + 调用 ApiClient + 响应校验
     */
    public DolphinSchedulerResponse<ProcessDefinition> createWorkflow(WorkflowRequest request) throws IOException {
        Map<String, String> params = buildWorkflowParams(request);
        DolphinSchedulerResponse<ProcessDefinition> response = client.createWorkflow(params);
        log.info("创建工作流: {}", toJson(response));
        return response;
    }

    /**
     * 更新工作流
     */
    public DolphinSchedulerResponse<Void> updateWorkflow(String workflowCode, WorkflowRequest request) throws IOException {
        Map<String, String> params = buildWorkflowParams(request);
        DolphinSchedulerResponse<Void> response = client.updateWorkflow(workflowCode, params);
        log.info("更新 [{}] 工作流: {}", workflowCode, toJson(response));
        return response;
    }

    /**
     * 删除工作流（需先下线）
     */
    public DolphinSchedulerResponse<Void> deleteWorkflow(String workflowCode) throws IOException {
        DolphinSchedulerResponse<Void> response = client.deleteWorkflow(workflowCode);
        log.info("删除 [{}] 工作流: {}", workflowCode, toJson(response));
        return response;
    }

    /**
     * 查询工作流详情
     */
    public DolphinSchedulerResponse<ProcessDefinitionDetail> getWorkflow(String workflowCode) throws IOException {
        DolphinSchedulerResponse<ProcessDefinitionDetail> response = client.workflowDetail(workflowCode);
        log.info("查询 [{}] 工作流详情：{}", workflowCode, toJson(response));
        return response;
    }

    /**
     * 查询所有工作流定义列表（分页 + 搜索）
     */
    public DolphinSchedulerResponse<PageInfo<ProcessDefinition>> listWorkflows(int pageNo, int pageSize, String searchVal) throws IOException {
        DolphinSchedulerResponse<PageInfo<ProcessDefinition>> response = client.listWorkflows(pageNo, pageSize, searchVal);
        log.info("查询工作流列表: {}", toJson(response));
        return response;
    }

    /**
     * 上线工作流
     */
    public DolphinSchedulerResponse<Void> onlineWorkflow(String workflowCode, String name) throws IOException {
        DolphinSchedulerResponse<Void> response = client.onlineWorkflow(workflowCode, name);
        log.info("上线 [{}] 工作流: {}", workflowCode, toJson(response));
        return response;
    }

    /**
     * 下线工作流
     */
    public DolphinSchedulerResponse<Void> offlineWorkflow(String workflowCode, String name) throws IOException {
        DolphinSchedulerResponse<Void> response = client.offlineWorkflow(workflowCode, name);
        log.info("下线 [{}] 工作流: {}", workflowCode, toJson(response));
        return response;
    }

    /**
     * 手动触发工作流执行
     */
    public DolphinSchedulerResponse<Integer> triggerWorkflow(String workflowCode) throws IOException {
        DolphinSchedulerResponse<Integer> response = client.triggerWorkflow(workflowCode);
        log.info("手动触发工作流: {}", toJson(response));
        return response;
    }

    /**
     * 将 WorkflowRequest DTO 转换为 ApiClient 需要的 Map 参数
     */
    private Map<String, String> buildWorkflowParams(WorkflowRequest request) {
        // 防御性处理：为 tasks 填充默认 version=1（DS 必填字段）
        if (request.getTasks() != null) {
            request.getTasks().forEach(task -> {
                if (task.getVersion() == null) {
                    task.setVersion(1L);
                }
            });
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("name", request.getName());
        params.put("description", request.getDescription() != null ? request.getDescription() : "");
        params.put("taskDefinitionJson", toJson(request.getTasks()));
        params.put("taskRelationJson", toJson(request.getTaskRelations()));
        params.put("executionType", request.getExecutionType());
        params.put("globalParams", request.getGlobalParams());
        params.put("timeout", String.valueOf(request.getTimeout()));
        return params;
    }
}
