package com.data.example.controller;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.bean.PageInfo;
import com.data.example.bean.ProcessInstance;
import com.data.example.service.WorkflowInstanceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * 功能：工作流实例管理 Controller
 *       负责工作流实例的查询、停止、恢复、暂停、删除等操作
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/21
 */
@Slf4j
@RestController
@RequestMapping("/api/workflow/instances")
public class WorkflowInstanceController {

    @Autowired
    private WorkflowInstanceService instanceService;

    /**
     * 查询工作流实例列表（分页 + 状态过滤）
     * GET /api/workflow/instances?pageNo=1&pageSize=10&stateType=RUNNING
     */
    @GetMapping
    public ResponseEntity<DolphinSchedulerResponse<PageInfo<ProcessInstance>>> listProcessInstances(
            @RequestParam(defaultValue = "1") int pageNo,
            @RequestParam(defaultValue = "10") int pageSize,
            @RequestParam(required = false) String searchVal,
            @RequestParam(required = false) String stateType) throws IOException {
        log.info("请求查询工作流实例列表: pageNo={}, pageSize={}, stateType={}", pageNo, pageSize, stateType);
        return ResponseEntity.ok(instanceService.listProcessInstances(pageNo, pageSize, searchVal, stateType));
    }

    /**
     * 查询工作流实例详情
     * GET /api/workflow/instances/{instanceId}
     */
    @GetMapping("/{instanceId}")
    public ResponseEntity<DolphinSchedulerResponse<ProcessInstance>> getProcessInstance(
            @PathVariable int instanceId) throws IOException {
        log.info("请求查询工作流实例详情: instanceId={}", instanceId);
        return ResponseEntity.ok(instanceService.getProcessInstance(instanceId));
    }

    /**
     * 停止工作流实例
     * POST /api/workflow/instances/{instanceId}/stop
     */
    @PostMapping("/{instanceId}/stop")
    public ResponseEntity<DolphinSchedulerResponse<Void>> stopProcessInstance(
            @PathVariable int instanceId) throws IOException {
        log.info("请求停止工作流实例: instanceId={}", instanceId);
        return ResponseEntity.ok(instanceService.stopProcessInstance(instanceId));
    }

    /**
     * 恢复工作流实例
     * POST /api/workflow/instances/{instanceId}/recover
     */
    @PostMapping("/{instanceId}/recover")
    public ResponseEntity<DolphinSchedulerResponse<Void>> recoverProcessInstance(
            @PathVariable int instanceId) throws IOException {
        log.info("请求恢复工作流实例: instanceId={}", instanceId);
        return ResponseEntity.ok(instanceService.recoverProcessInstance(instanceId));
    }

    /**
     * 暂停工作流实例
     * POST /api/workflow/instances/{instanceId}/pause
     */
    @PostMapping("/{instanceId}/pause")
    public ResponseEntity<DolphinSchedulerResponse<Void>> pauseProcessInstance(
            @PathVariable int instanceId) throws IOException {
        log.info("请求暂停工作流实例: instanceId={}", instanceId);
        return ResponseEntity.ok(instanceService.pauseProcessInstance(instanceId));
    }

    /**
     * 删除工作流实例
     * DELETE /api/workflow/instances/{instanceId}
     */
    @DeleteMapping("/{instanceId}")
    public ResponseEntity<DolphinSchedulerResponse<Void>> deleteProcessInstance(
            @PathVariable int instanceId) throws IOException {
        log.info("请求删除工作流实例: instanceId={}", instanceId);
        return ResponseEntity.ok(instanceService.deleteProcessInstance(instanceId));
    }
}
