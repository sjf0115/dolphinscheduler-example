package com.data.example.service;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.client.DolphinSchedulerApiClient;
import com.data.example.dto.WorkflowRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.data.example.utils.CommonUtil.parseResponse;
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
    public DolphinSchedulerResponse<?> createWorkflow(WorkflowRequest request) throws IOException {
        Map<String, String> params = buildWorkflowParams(request);
        String response = client.createWorkflow(params);
        log.info("创建工作流: {}", response);
        return parseResponse(response);
    }

    /**
     * 更新工作流
     */
    public DolphinSchedulerResponse<?> updateWorkflow(String workflowCode, WorkflowRequest request) throws IOException {
        log.info("[Service] 更新工作流: code={}, name={}", workflowCode, request.getName());
        Map<String, String> params = buildWorkflowParams(request);
        String response = client.updateWorkflow(workflowCode, params);
        return parseResponse(response);
    }

    /**
     * 删除工作流（需先下线）
     */
    public DolphinSchedulerResponse<?> deleteWorkflow(String workflowCode) throws IOException {
        String response = client.deleteWorkflow(workflowCode);
        log.info("删除 [{}] 工作流: {}", workflowCode, response);
        return parseResponse(response);
    }

    /**
     * 查询工作流详情
     */
    public DolphinSchedulerResponse<?> getWorkflow(String workflowCode) throws IOException {
        String response = client.workflowDetail(workflowCode);
        log.info("查询 [{}] 工作流详情：{}", workflowCode, response);
        return parseResponse(response);
    }

    /**
     * 查询所有工作流定义列表（分页 + 搜索）
     */
    public DolphinSchedulerResponse<?> listWorkflows(int pageNo, int pageSize, String searchVal) throws IOException {
        String response = client.listWorkflows(pageNo, pageSize, searchVal);
        log.info("查询工作流列表: {}", response);
        return parseResponse(response);
    }

    /**
     * 上线工作流
     */
    public DolphinSchedulerResponse<?> onlineWorkflow(String workflowCode, String name) throws IOException {
        String response = client.onlineWorkflow(workflowCode, name);
        log.info("上线 [{}] 工作流: {}", workflowCode, response);
        return parseResponse(response);
    }

    /**
     * 下线工作流
     */
    public DolphinSchedulerResponse<?> offlineWorkflow(String workflowCode, String name) throws IOException {
        String response = client.offlineWorkflow(workflowCode, name);
        log.info("下线 [{}] 工作流: {}", workflowCode, response);
        return parseResponse(response);
    }

    /**
     * 手动触发工作流执行
     */
    public DolphinSchedulerResponse<?> triggerWorkflow(String workflowCode) throws IOException {
        String response = client.triggerWorkflow(workflowCode);
        log.info("手动触发工作流: {}", response);
        return parseResponse(response);
    }



    // ==================== 定时调度 ====================

    // 创建定时调度
    public DolphinSchedulerResponse<?> createSchedule(String workflowCode, String scheduleJson) throws IOException {
        String response = client.createSchedule(workflowCode, scheduleJson);
        log.info("为 [{}] 工作流创建定时调度: {}", workflowCode, response);
        return parseResponse(response);
    }

    // 上线调度
    public DolphinSchedulerResponse<?> onlineSchedule(String scheduleId) throws IOException {
        String response = client.onlineSchedule(scheduleId);
        log.info("上线 [{}] 调度: {}", scheduleId, response);
        return parseResponse(response);
    }

    // 下线调度
    public DolphinSchedulerResponse<?> offlineSchedule(String scheduleId) throws IOException {
        String response = client.offlineSchedule(scheduleId);
        log.info("下线 [{}] 调度: {}", scheduleId, response);
        return parseResponse(response);
    }

    // ==================== 内部方法 ====================

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
