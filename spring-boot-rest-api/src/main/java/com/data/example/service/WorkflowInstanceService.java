package com.data.example.service;

import com.data.example.bean.DolphinSchedulerResponse;
import com.data.example.client.DolphinSchedulerApiClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static com.data.example.utils.CommonUtil.parseResponse;

/**
 * 功能：工作流实例业务 Service
 *       负责工作流实例的查询、停止、恢复、暂停、删除等操作
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/21
 */
@Slf4j
@Service
public class WorkflowInstanceService {

    @Autowired
    private DolphinSchedulerApiClient client;

    /**
     * 查询工作流实例列表（分页 + 状态过滤）
     */
    public DolphinSchedulerResponse<?> listProcessInstances(int pageNo, int pageSize, String searchVal, String stateType) throws IOException {
        String response = client.listProcessInstances(pageNo, pageSize, searchVal, stateType);
        log.info("查询工作流实例列表: {}", response);
        return parseResponse(response);
    }

    /**
     * 查询工作流实例详情
     */
    public DolphinSchedulerResponse<?> getProcessInstance(int instanceId) throws IOException {
        String response = client.processInstanceDetail(instanceId);
        log.info("查询工作流实例 [{}] 详情: {}", instanceId, response);
        return parseResponse(response);
    }

    /**
     * 停止工作流实例
     */
    public DolphinSchedulerResponse<?> stopProcessInstance(int instanceId) throws IOException {
        String response = client.executeProcessInstance(instanceId, "STOP");
        log.info("停止工作流实例 [{}]: {}", instanceId, response);
        return parseResponse(response);
    }

    /**
     * 恢复工作流实例
     */
    public DolphinSchedulerResponse<?> recoverProcessInstance(int instanceId) throws IOException {
        String response = client.executeProcessInstance(instanceId, "RECOVER_TOLERANCE");
        log.info("恢复工作流实例 [{}]: {}", instanceId, response);
        return parseResponse(response);
    }

    /**
     * 暂停工作流实例
     */
    public DolphinSchedulerResponse<?> pauseProcessInstance(int instanceId) throws IOException {
        String response = client.executeProcessInstance(instanceId, "PAUSE");
        log.info("暂停工作流实例 [{}]: {}", instanceId, response);
        return parseResponse(response);
    }

    /**
     * 删除工作流实例
     */
    public DolphinSchedulerResponse<?> deleteProcessInstance(int instanceId) throws IOException {
        String response = client.deleteProcessInstance(instanceId);
        log.info("删除工作流实例 [{}]: {}", instanceId, response);
        return parseResponse(response);
    }
}
