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
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/20
 */
@Slf4j
@Service
public class DolphinSchedulerService {
    @Autowired
    private DolphinSchedulerApiClient client;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private ScheduleService scheduleService;

    /**
     * 一键发布工作流：创建 → 上线 → 创建调度 → 上线调度
     * 
     * 这是 Service 层的核心价值之一 —— 编排多个 API 调用为原子操作
     */
    public DolphinSchedulerResponse<?> deployWorkflow(WorkflowRequest request, String cron) throws IOException {
        log.info("[Service] 一键发布工作流: name={}, cron={}", request.getName(), cron);

        // Step 1: 创建工作流
        DolphinSchedulerResponse<?> createResp = workflowService.createWorkflow(request);
        if (!createResp.isSuccess()) {
            log.error("创建工作流失败: {}", createResp.getMsg());
            return createResp;
        }

        // 从响应中解析工作流 code
        String workflowCode = extractWorkflowCode(createResp);
        if (workflowCode == null) {
            log.error("无法从创建响应中解析工作流 code");
            return new DolphinSchedulerResponse<>(-1, "创建工作流后无法获取 code", null, false);
        }
        log.info("[Service] 工作流创建成功: code={}", workflowCode);

        // Step 2: 上线工作流
        DolphinSchedulerResponse<?> onlineResp = workflowService.onlineWorkflow(workflowCode, request.getName());
        if (!onlineResp.isSuccess()) {
            log.error("上线工作流失败: {}", onlineResp.getMsg());
            return onlineResp;
        }
        log.info("[Service] 工作流上线成功: code={}", workflowCode);

        // Step 3: 如果有 cron 表达式，创建定时调度并上线
        if (cron != null && !cron.isEmpty()) {
            String scheduleJson = buildScheduleJson(cron);
            DolphinSchedulerResponse<?> scheduleResp = scheduleService.createSchedule(workflowCode, scheduleJson);
            if (!scheduleResp.isSuccess()) {
                log.error("创建调度失败: {}", scheduleResp.getMsg());
                return scheduleResp;
            }

            String scheduleId = extractScheduleId(scheduleResp);
            if (scheduleId != null) {
                DolphinSchedulerResponse<?> onlineSchedResp = scheduleService.onlineSchedule(scheduleId);
                if (!onlineSchedResp.isSuccess()) {
                    log.error("上线调度失败: {}", onlineSchedResp.getMsg());
                    return onlineSchedResp;
                }
                log.info("[Service] 定时调度上线成功: scheduleId={}", scheduleId);
            }
        }

        log.info("[Service] 一键发布完成: workflowCode={}", workflowCode);
        return new DolphinSchedulerResponse<>(0, "发布成功", workflowCode, true);
    }

    // ==================== 辅助接口 ====================

    /**
     * 生成 Task Code
     */
    public DolphinSchedulerResponse<?> genTaskCodes(int genNum) throws IOException {
        String taskCodes = client.genTaskCodes(genNum);
        log.info("生成 Task Code: {}", taskCodes);
        return parseResponse(taskCodes);
    }

    /**
     * 测试 DolphinScheduler 连通性
     */
    public DolphinSchedulerResponse<?> healthCheck() throws IOException {
        log.info("[Service] 测试连通性");
        return parseResponse(client.testConnection());
    }

    // ==================== 内部方法 ====================

    /**
     * 将 WorkflowRequest DTO 转换为 ApiClient 需要的 Map 参数
     * 这是 Service 层的核心职责之一 —— 隔离 DTO 和底层 API 参数
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

    /**
     * 构建定时调度 JSON
     */
    private String buildScheduleJson(String cron) {
        Map<String, String> schedule = new LinkedHashMap<>();
        schedule.put("startTime", "2026-01-01 00:00:00");
        schedule.put("endTime", "2099-12-31 23:59:59");
        schedule.put("crontab", cron);
        schedule.put("timezoneId", "Asia/Shanghai");
        return toJson(schedule);
    }



    /**
     * 从创建工作流的响应中提取 workflow code
     */
    private String extractWorkflowCode(DolphinSchedulerResponse<?> resp) {
        Object data = resp.getData();
        if (data == null) return null;
        if (data instanceof Number) {
            return String.valueOf(data);
        }
        if (data instanceof Map) {
            Object code = ((Map<?, ?>) data).get("code");
            return code != null ? String.valueOf(code) : null;
        }
        return String.valueOf(data);
    }

    /**
     * 从创建调度的响应中提取 schedule id
     */
    private String extractScheduleId(DolphinSchedulerResponse<?> resp) {
        Object data = resp.getData();
        if (data == null) return null;
        if (data instanceof Number) {
            return String.valueOf(data);
        }
        if (data instanceof Map) {
            Object id = ((Map<?, ?>) data).get("id");
            return id != null ? String.valueOf(id) : null;
        }
        return String.valueOf(data);
    }
}
