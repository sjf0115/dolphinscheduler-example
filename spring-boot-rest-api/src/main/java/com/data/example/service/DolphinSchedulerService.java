package com.data.example.service;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.bean.ProcessDefinition;
import com.data.example.bean.Schedule;
import com.data.example.client.DolphinSchedulerApiClient;
import com.data.example.dto.WorkflowRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.data.example.utils.CommonUtil.toJson;

/**
 * 功能：DolphinScheduler 综合业务 Service
 *       负责多 API 编排组合（如一键发布）及辅助接口
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
    public DolphinSchedulerResponse<String> deployWorkflow(WorkflowRequest request, String cron) throws IOException {
        // Step 1: 创建工作流
        DolphinSchedulerResponse<ProcessDefinition> createResp = workflowService.createWorkflow(request);
        if (!createResp.isSuccess()) {
            log.error("创建工作流失败: {}", createResp.getMsg());
            return new DolphinSchedulerResponse<>(-1, createResp.getMsg(), null, false);
        }

        // 从响应中获取工作流 code
        ProcessDefinition processDef = createResp.getData();
        if (processDef == null || processDef.getCode() == null) {
            log.error("无法从创建响应中获取工作流 code");
            return new DolphinSchedulerResponse<>(-1, "创建工作流后无法获取 code", null, false);
        }
        String workflowCode = String.valueOf(processDef.getCode());
        log.info("工作流 [{}] 创建成功", workflowCode);

        // Step 2: 上线工作流
        DolphinSchedulerResponse<Void> onlineResp = workflowService.onlineWorkflow(workflowCode, request.getName());
        if (!onlineResp.isSuccess()) {
            log.error("上线工作流  [{}] 失败: {}", workflowCode, onlineResp.getMsg());
            return new DolphinSchedulerResponse<>(-1, onlineResp.getMsg(), null, false);
        }
        log.info("工作流 [{}] 上线成功", workflowCode);

        // Step 3: 如果有 cron 表达式，创建定时调度并上线
        if (cron != null && !cron.isEmpty()) {
            String scheduleJson = buildScheduleJson(cron);
            DolphinSchedulerResponse<Schedule> scheduleResp = scheduleService.createSchedule(workflowCode, scheduleJson);
            if (!scheduleResp.isSuccess()) {
                log.error("为工作流 [{}] 创建调度失败: {}", workflowCode, scheduleResp.getMsg());
                return new DolphinSchedulerResponse<>(-1, scheduleResp.getMsg(), null, false);
            }

            Schedule schedule = scheduleResp.getData();
            if (schedule != null && schedule.getId() != null) {
                String scheduleId = String.valueOf(schedule.getId());
                DolphinSchedulerResponse<Void> onlineScheduleResp = scheduleService.onlineSchedule(scheduleId);
                if (!onlineScheduleResp.isSuccess()) {
                    log.error("上线调度失败: {}", onlineScheduleResp.getMsg());
                    return new DolphinSchedulerResponse<>(-1, onlineScheduleResp.getMsg(), null, false);
                }
                log.info("定时调度 [{}] 上线成功", scheduleId);
            }
        }

        log.info("工作流 [{}] 发布完成", workflowCode);
        return new DolphinSchedulerResponse<>(0, "发布成功", workflowCode, true);
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
     * 安全删除工作流：下线调度 → 删除调度 → 下线工作流 → 删除工作流
     */
    public DolphinSchedulerResponse<Void> safeDeleteWorkflow(String workflowCode) throws IOException {
        long code = Long.parseLong(workflowCode);

        // Step 1: 查找并清理关联的定时调度
        DolphinSchedulerResponse<List<Schedule>> schedResp = scheduleService.listSchedules();
        if (schedResp.isSuccess() && schedResp.getData() != null) {
            for (Schedule schedule : schedResp.getData()) {
                if (code == schedule.getProcessDefinitionCode() && schedule.getId() != null) {
                    String scheduleId = String.valueOf(schedule.getId());
                    // 先下线调度（忽略失败，可能已经是 OFFLINE）
                    scheduleService.offlineSchedule(scheduleId);
                    // 再删除调度
                    DolphinSchedulerResponse<Void> delSchedResp = scheduleService.deleteSchedule(scheduleId);
                    if (!delSchedResp.isSuccess()) {
                        log.warn("删除调度 [{}] 失败: {}", scheduleId, delSchedResp.getMsg());
                    } else {
                        log.info("调度 [{}] 已清理", scheduleId);
                    }
                }
            }
        }

        // Step 2: 下线工作流（需要先获取名称）
        DolphinSchedulerResponse<?> detailResp = workflowService.getWorkflow(workflowCode);
        String name = null;
        if (detailResp.getData() != null) {
            Object data = detailResp.getData();
            if (data instanceof com.data.example.bean.ProcessDefinitionDetail) {
                ProcessDefinition pd = ((com.data.example.bean.ProcessDefinitionDetail) data).getProcessDefinition();
                if (pd != null) name = pd.getName();
            }
        }
        workflowService.offlineWorkflow(workflowCode, name);

        // Step 3: 删除工作流
        DolphinSchedulerResponse<Void> deleteResp = workflowService.deleteWorkflow(workflowCode);
        if (!deleteResp.isSuccess()) {
            log.error("删除工作流 [{}] 失败: {}", workflowCode, deleteResp.getMsg());
            return deleteResp;
        }

        log.info("工作流 [{}] 安全删除成功", workflowCode);
        return new DolphinSchedulerResponse<>(0, "删除成功", null, true);
    }
}
