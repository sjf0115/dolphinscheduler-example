package com.data.example.controller;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.dto.WorkflowRequest;
import com.data.example.service.DolphinSchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * 功能：DolphinScheduler 综合接口
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/21 15:30
 */
@Slf4j
@RestController
@RequestMapping("/api")
public class DolphinSchedulerController {
    @Autowired
    private DolphinSchedulerService service;

    /**
     * 一键发布工作流：创建 → 上线 → 创建调度 → 上线调度
     * POST /api/deploy?cron=0+0%2F1+*+*+*+%3F+*
     * Body: WorkflowRequest JSON
     */
    @PostMapping("/deploy")
    public ResponseEntity<DolphinSchedulerResponse<String>> deploy(
            @RequestBody WorkflowRequest request,
            @RequestParam(required = false) String cron) throws IOException {
        return ResponseEntity.ok(service.deployWorkflow(request, cron));
    }

    /**
     * 安全删除工作流：自动清理调度 → 下线 → 删除
     * DELETE /api/deploy/{workflowCode}
     */
    @DeleteMapping("/deploy/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<Void>> safeDelete(@PathVariable String workflowCode) throws IOException {
        return ResponseEntity.ok(service.safeDeleteWorkflow(workflowCode));
    }
}
