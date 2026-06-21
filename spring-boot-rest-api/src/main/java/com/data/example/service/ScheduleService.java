package com.data.example.service;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.client.DolphinSchedulerApiClient;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static com.data.example.utils.CommonUtil.parseResponse;

/**
 * 功能：调度服务
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/20
 */
@Slf4j
@Service
public class ScheduleService {
    @Autowired
    private DolphinSchedulerApiClient client;

    /**
     * 创建定时调度
     */
    public DolphinSchedulerResponse<?> createSchedule(String workflowCode, String scheduleJson) throws IOException {
        String response = client.createSchedule(workflowCode, scheduleJson);
        log.info("为 [{}] 工作流创建定时调度: {}", workflowCode, response);
        return parseResponse(response);
    }

    /**
     * 所有调度
     */
    public DolphinSchedulerResponse<?> listSchedules() throws IOException {
        String response = client.listSchedules();
        log.info("所有定时调度: {}", response);
        return parseResponse(response);
    }

    /**
     * 上线调度
     */
    public DolphinSchedulerResponse<?> onlineSchedule(String scheduleId) throws IOException {
        String response = client.onlineSchedule(scheduleId);
        log.info("上线 [{}] 调度: {}", scheduleId, response);
        return parseResponse(response);
    }

    /**
     * 下线调度
     */
    public DolphinSchedulerResponse<?> offlineSchedule(String scheduleId) throws IOException {
        String response = client.offlineSchedule(scheduleId);
        log.info("下线 [{}] 调度: {}", scheduleId, response);
        return parseResponse(response);
    }

    /**
     * 删除调度
     */
    public DolphinSchedulerResponse<?> deleteSchedule(String scheduleId) throws IOException {
        String response = client.deleteSchedule(scheduleId);
        log.info("删除 [{}] 调度: {}", scheduleId, response);
        return parseResponse(response);
    }
}
