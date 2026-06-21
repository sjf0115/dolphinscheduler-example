package com.data.example.controller;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.service.ScheduleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * 功能：调度示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/21 11:28
 */
@Slf4j
@RestController
@RequestMapping("/api/schedule")
public class ScheduleController {

    @Autowired
    private ScheduleService scheduleService;

    /**
     * 创建定时调度
     * POST /api/workflow/{workflowCode}/schedule
     *
     * 请求参数 scheduleJson 示例：
     * <pre>
     * {
     *   "startTime": "2026-01-01 00:00:00",
     *   "endTime": "2099-12-31 23:59:59",
     *   "crontab": "0 0 2 * * ? *",
     *   "timezoneId": "Asia/Shanghai"
     * }
     * </pre>
     */
    @PostMapping("/{workflowCode}")
    public ResponseEntity<DolphinSchedulerResponse<?>> createSchedule(
            @PathVariable String workflowCode,
            @RequestBody String scheduleJson) throws IOException {
        log.info("请求为 [{}] 工作流创建定时调度: {}", workflowCode, scheduleJson);
        return ResponseEntity.ok(scheduleService.createSchedule(workflowCode, scheduleJson));
    }

    // 所有调度
    @GetMapping("/list")
    public ResponseEntity<DolphinSchedulerResponse<?>> listSchedules() throws IOException {
        log.info("请求所有定时调度");
        return ResponseEntity.ok(scheduleService.listSchedules());
    }

    // 上线调度
    @PostMapping("/{scheduleId}/online")
    public ResponseEntity<DolphinSchedulerResponse<?>> onlineSchedule(
            @PathVariable String scheduleId) throws IOException {
        return ResponseEntity.ok(scheduleService.onlineSchedule(scheduleId));
    }

    // 下线调度
    @PostMapping("/{scheduleId}/offline")
    public ResponseEntity<DolphinSchedulerResponse<?>> offlineSchedule(
            @PathVariable String scheduleId) throws IOException {
        return ResponseEntity.ok(scheduleService.offlineSchedule(scheduleId));
    }

    // 删除调度
    @DeleteMapping("/{scheduleId}")
    public ResponseEntity<DolphinSchedulerResponse<?>> deleteSchedule(@PathVariable String scheduleId) throws IOException {
        return ResponseEntity.ok(scheduleService.deleteSchedule(scheduleId));
    }
}
