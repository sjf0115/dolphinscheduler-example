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
 * 功能：DolphinScheduler 实例
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

    @PostMapping("/deploy")
    public ResponseEntity<DolphinSchedulerResponse<?>> deploy(
            @RequestBody WorkflowRequest request,
            @RequestBody String cron) throws IOException {
        return ResponseEntity.ok(service.deployWorkflow(request, cron));
    }
}
