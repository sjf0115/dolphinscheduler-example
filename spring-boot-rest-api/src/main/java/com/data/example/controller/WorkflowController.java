package com.data.example.controller;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.dto.WorkflowRequest;
import com.data.example.service.WorkflowService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * 功能：工作流管理 Controller
 *       只负责 HTTP 请求/响应映射，业务逻辑全部委托给 WorkflowService
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/20
 */
@Slf4j
@RestController
@RequestMapping("/api/workflow")
public class WorkflowController {
    private static final Gson gson = new GsonBuilder().create();
    @Autowired
    private WorkflowService workflowService;

    // ==================== 工作流定义 CRUD ====================

    /**
     * 创建工作流
     * POST /api/workflow
     *
     * 请求体示例：
     * <pre>
     * {
     *   "name": "my_workflow",
     *   "description": "示例工作流",
     *   "executionType": "PARALLEL",
     *   "tasks": [
     *     {"code": 123, "name": "task_a", "taskType": "SHELL",
     *      "taskParams": "{\"rawScript\":\"echo hello\"}", "flag": "YES", "timeout": 0},
     *     {"code": 456, "name": "task_b", "taskType": "SHELL",
     *      "taskParams": "{\"rawScript\":\"echo world\"}", "flag": "YES", "timeout": 0}
     *   ],
     *   "taskRelations": [
     *     {"name": "", "preTaskCode": 0,   "preTaskVersion": 0,
     *      "postTaskCode": 123, "postTaskVersion": 1, "conditionType": "NONE"},
     *     {"name": "", "preTaskCode": 123, "preTaskVersion": 1,
     *      "postTaskCode": 456, "postTaskVersion": 1, "conditionType": "NONE"}
     *   ]
     * }
     * </pre>
     */
    @PostMapping
    public ResponseEntity<DolphinSchedulerResponse<?>> createWorkflow(@RequestBody WorkflowRequest request) throws IOException {
        log.info("请求创建工作流：{}", gson.toJson(request));
        return ResponseEntity.ok(workflowService.createWorkflow(request));
    }

    /**
     * 更新工作流
     * PUT /api/workflow/{workflowCode}
     */
    @PutMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> updateWorkflow(
            @PathVariable String workflowCode,
            @RequestBody WorkflowRequest request) throws IOException {
        return ResponseEntity.ok(workflowService.updateWorkflow(workflowCode, request));
    }

    /**
     * 查询工作流详情
     * GET /api/workflow/{workflowCode}
     */
    @GetMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> getWorkflow(@PathVariable String workflowCode) throws IOException {
        log.info("请求查询工作流详情：{}", workflowCode);
        return ResponseEntity.ok(workflowService.getWorkflow(workflowCode));
    }

    /**
     * 删除工作流（需先下线）
     * DELETE /api/workflow/{workflowCode}
     */
    @DeleteMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> deleteWorkflow(@PathVariable String workflowCode) throws IOException {
        return ResponseEntity.ok(workflowService.deleteWorkflow(workflowCode));
    }

    // ==================== 上线/下线 ====================

    /**
     * 上线工作流
     * POST /api/workflow/{workflowCode}/online
     */
    @PostMapping("/{workflowCode}/online")
    public ResponseEntity<DolphinSchedulerResponse<?>> onlineWorkflow(@PathVariable String workflowCode,
            @RequestParam String name) throws IOException {
        log.info("请求上线 [{}] 工作流", workflowCode);
        return ResponseEntity.ok(workflowService.onlineWorkflow(workflowCode, name));
    }

    /**
     * 下线工作流
     * POST /api/workflow/{workflowCode}/offline
     */
    @PostMapping("/{workflowCode}/offline")
    public ResponseEntity<DolphinSchedulerResponse<?>> offlineWorkflow(@PathVariable String workflowCode,
            @RequestParam String name) throws IOException {
        log.info("请求下线 [{}] 工作流", workflowCode);
        return ResponseEntity.ok(workflowService.offlineWorkflow(workflowCode, name));
    }

    // ==================== 手动触发执行 ====================

    /**
     * 手动触发工作流执行
     * POST /api/workflow/{workflowCode}/trigger
     */
    @PostMapping("/{workflowCode}/trigger")
    public ResponseEntity<DolphinSchedulerResponse<?>> triggerWorkflow(
            @PathVariable String workflowCode) throws IOException {
        return ResponseEntity.ok(workflowService.triggerWorkflow(workflowCode));
    }



    // ==================== 复合操作 ====================

    /**
     * 一键发布工作流：创建 → 上线 → 创建调度 → 上线调度
     * POST /api/workflow/deploy?cron=0+0+2+*+*+%3F+*
     */
    @PostMapping("/deploy")
    public ResponseEntity<DolphinSchedulerResponse<?>> deployWorkflow(
            @RequestBody WorkflowRequest request,
            @RequestParam(required = false) String cron) throws IOException {
        return ResponseEntity.ok(workflowService.deployWorkflow(request, cron));
    }

    // ==================== 辅助接口 ====================

    /**
     * 生成 Task Code（创建任务前必须先调用）
     * GET /api/workflow/gen-task-codes?genNum=2
     */
    @GetMapping("/gen-task-codes")
    public ResponseEntity<DolphinSchedulerResponse<?>> genTaskCodes(@RequestParam(defaultValue = "1") int genNum) throws IOException {
        return ResponseEntity.ok(workflowService.genTaskCodes(genNum));
    }

    /**
     * 测试 DolphinScheduler 连通性
     * GET /api/workflow/health
     */
    @GetMapping("/health")
    public ResponseEntity<DolphinSchedulerResponse<?>> healthCheck() throws IOException {
        return ResponseEntity.ok(workflowService.healthCheck());
    }
}
