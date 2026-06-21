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

    // 查询所有工作流定义列表（分页 + 搜索）
    @GetMapping
    public ResponseEntity<DolphinSchedulerResponse<?>> listWorkflows(
            @RequestParam(defaultValue = "1") int pageNo,
            @RequestParam(defaultValue = "10") int pageSize,
            @RequestParam(required = false) String searchVal) throws IOException {
        log.info("请求查询工作流列表: pageNo={}, pageSize={}, searchVal={}", pageNo, pageSize, searchVal);
        return ResponseEntity.ok(workflowService.listWorkflows(pageNo, pageSize, searchVal));
    }

    // 查询工作流详情
    @GetMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> getWorkflow(@PathVariable String workflowCode) throws IOException {
        log.info("请求查询工作流详情：{}", workflowCode);
        return ResponseEntity.ok(workflowService.getWorkflow(workflowCode));
    }

    // 更新工作流
    @PutMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> updateWorkflow(
            @PathVariable String workflowCode,
            @RequestBody WorkflowRequest request) throws IOException {
        return ResponseEntity.ok(workflowService.updateWorkflow(workflowCode, request));
    }

    // 删除工作流（需先下线）
    @DeleteMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> deleteWorkflow(@PathVariable String workflowCode) throws IOException {
        log.info("请求删除 [{}] 工作流", workflowCode);
        return ResponseEntity.ok(workflowService.deleteWorkflow(workflowCode));
    }

    // 上线工作流
    @PostMapping("/{workflowCode}/online")
    public ResponseEntity<DolphinSchedulerResponse<?>> onlineWorkflow(@PathVariable String workflowCode,
            @RequestParam String name) throws IOException {
        log.info("请求上线 [{}] 工作流", workflowCode);
        return ResponseEntity.ok(workflowService.onlineWorkflow(workflowCode, name));
    }

    // 下线工作流
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
}
