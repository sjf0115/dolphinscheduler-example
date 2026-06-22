package com.data.example.service;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.bean.Schedule;
import com.data.example.client.DolphinSchedulerApiClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

import static com.data.example.utils.CommonUtil.toJson;

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
    public DolphinSchedulerResponse<Schedule> createSchedule(String workflowCode, String scheduleJson) throws IOException {
        DolphinSchedulerResponse<Schedule> response = client.createSchedule(workflowCode, scheduleJson);
        log.info("为 [{}] 工作流创建定时调度: {}", workflowCode, toJson(response));
        return response;
    }

    /**
     * 所有调度
     */
    public DolphinSchedulerResponse<List<Schedule>> listSchedules() throws IOException {
        DolphinSchedulerResponse<List<Schedule>> response = client.listSchedules();
        log.info("所有定时调度: {}", toJson(response));
        return response;
    }

    /**
     * 上线调度
     */
    public DolphinSchedulerResponse<Void> onlineSchedule(String scheduleId) throws IOException {
        DolphinSchedulerResponse<Void> response = client.onlineSchedule(scheduleId);
        log.info("上线 [{}] 调度: {}", scheduleId, toJson(response));
        return response;
    }

    /**
     * 下线调度
     */
    public DolphinSchedulerResponse<Void> offlineSchedule(String scheduleId) throws IOException {
        DolphinSchedulerResponse<Void> response = client.offlineSchedule(scheduleId);
        log.info("下线 [{}] 调度: {}", scheduleId, toJson(response));
        return response;
    }

    /**
     * 删除调度
     */
    public DolphinSchedulerResponse<Void> deleteSchedule(String scheduleId) throws IOException {
        DolphinSchedulerResponse<Void> response = client.deleteSchedule(scheduleId);
        log.info("删除 [{}] 调度: {}", scheduleId, toJson(response));
        return response;
    }
}
