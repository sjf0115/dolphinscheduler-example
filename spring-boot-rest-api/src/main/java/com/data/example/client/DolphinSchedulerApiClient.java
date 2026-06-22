package com.data.example.client;

import com.data.example.bean.*;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.*;

import static com.data.example.utils.CommonUtil.parseResponse;

/**
 * DolphinScheduler OpenAPI 封装。
 *
 * <p>基于 HttpClient 调用 DolphinScheduler RESTful API。
 * 配置项从 Engine 表的 config 字段读取：apiUrl、token、projectCode、tenantCode。</p>
 *
 * <p>参考 Swagger 文档：/dolphinscheduler/swagger-ui/index.html#/process%20definition%20related%20operation</p>
 */
@Slf4j
public class DolphinSchedulerApiClient {

    // ==================== 泛型 TypeReference 常量 ====================
    private static final TypeReference<DolphinSchedulerResponse<ProcessDefinition>> RESP_PROCESS_DEF =
            new TypeReference<DolphinSchedulerResponse<ProcessDefinition>>() {};
    private static final TypeReference<DolphinSchedulerResponse<ProcessDefinitionDetail>> RESP_PROCESS_DEF_DETAIL =
            new TypeReference<DolphinSchedulerResponse<ProcessDefinitionDetail>>() {};
    private static final TypeReference<DolphinSchedulerResponse<PageInfo<ProcessDefinition>>> RESP_PAGE_PROCESS_DEF =
            new TypeReference<DolphinSchedulerResponse<PageInfo<ProcessDefinition>>>() {};
    private static final TypeReference<DolphinSchedulerResponse<Void>> RESP_VOID =
            new TypeReference<DolphinSchedulerResponse<Void>>() {};
    private static final TypeReference<DolphinSchedulerResponse<Integer>> RESP_INT =
            new TypeReference<DolphinSchedulerResponse<Integer>>() {};
    private static final TypeReference<DolphinSchedulerResponse<Schedule>> RESP_SCHEDULE =
            new TypeReference<DolphinSchedulerResponse<Schedule>>() {};
    private static final TypeReference<DolphinSchedulerResponse<List<Schedule>>> RESP_SCHEDULE_LIST =
            new TypeReference<DolphinSchedulerResponse<List<Schedule>>>() {};
    private static final TypeReference<DolphinSchedulerResponse<PageInfo<ProcessInstance>>> RESP_PAGE_PROCESS_INST =
            new TypeReference<DolphinSchedulerResponse<PageInfo<ProcessInstance>>>() {};
    private static final TypeReference<DolphinSchedulerResponse<ProcessInstance>> RESP_PROCESS_INST =
            new TypeReference<DolphinSchedulerResponse<ProcessInstance>>() {};
    private static final TypeReference<DolphinSchedulerResponse<List<Long>>> RESP_LONG_LIST =
            new TypeReference<DolphinSchedulerResponse<List<Long>>>() {};
    private static final TypeReference<DolphinSchedulerResponse<Map<String, Object>>> RESP_MAP =
            new TypeReference<DolphinSchedulerResponse<Map<String, Object>>>() {};

    private String apiUrl;
    private String token;
    private String projectCode;
    private String tenantCode;

    /** 初始化 API 客户端配置 */
    public void init(Map<String, Object> config) {
        this.apiUrl = getString(config, "apiUrl");
        this.token = getString(config, "token");
        this.projectCode = getString(config, "projectCode");
        this.tenantCode = getString(config, "tenantCode");
        if (this.tenantCode == null || this.tenantCode.isEmpty()) {
            this.tenantCode = "default";
        }
        log.info("DolphinSchedulerApiClient 初始化: apiUrl={}, projectCode={}, tenantCode={}", apiUrl, projectCode, tenantCode);
    }

    //------------------------------------------------------------------------------------------------------------------
    // 1. 工作流定义

    /**
     * 创建工作流定义
     * POST /projects/{projectCode}/process-definition
     */
    public DolphinSchedulerResponse<ProcessDefinition> createWorkflow(Map<String, String> params) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition";
        params.putIfAbsent("tenantCode", tenantCode);
        params.putIfAbsent("locations", "[]");
        params.putIfAbsent("globalParams", "[]");
        params.putIfAbsent("timeout", "0");
        return parseResponse(doPostForm(url, params), RESP_PROCESS_DEF);
    }

    /**
     * 查询工作流定义列表
     * GET /projects/{projectCode}/process-definition?pageNo=1&pageSize=10&searchVal=
     */
    public DolphinSchedulerResponse<PageInfo<ProcessDefinition>> listWorkflows(int pageNo, int pageSize, String searchVal) throws IOException {
        StringBuilder url = new StringBuilder(apiUrl + "/projects/" + projectCode + "/process-definition");
        url.append("?pageNo=").append(pageNo);
        url.append("&pageSize=").append(pageSize);
        if (searchVal != null && !searchVal.isEmpty()) {
            url.append("&searchVal=").append(searchVal);
        }
        return parseResponse(doGet(url.toString()), RESP_PAGE_PROCESS_DEF);
    }

    /**
     * 手动触发工作流执行
     * POST /projects/{projectCode}/executors/start-process-instance
     */
    public DolphinSchedulerResponse<Integer> triggerWorkflow(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/executors/start-process-instance";
        Map<String, String> params = new LinkedHashMap<>();
        params.put("processDefinitionCode", workflowCode);
        params.put("scheduleTime", "[]");
        params.put("failureStrategy", "CONTINUE");
        params.put("warningType", "NONE");
        params.put("warningGroupId", "0");
        params.put("processInstancePriority", "MEDIUM");
        params.put("runMode", "RUN_MODE_SERIAL");
        return parseResponse(doPostForm(url, params), RESP_INT);
    }

    /** 查询工作流详情 */
    public DolphinSchedulerResponse<ProcessDefinitionDetail> workflowDetail(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode;
        return parseResponse(doGet(url), RESP_PROCESS_DEF_DETAIL);
    }

    /**
     * 更新工作流定义
     * PUT /projects/{projectCode}/process-definition/{code}
     */
    public DolphinSchedulerResponse<Void> updateWorkflow(String workflowCode, Map<String, String> params) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode;
        params.putIfAbsent("tenantCode", tenantCode);
        params.putIfAbsent("locations", "[]");
        params.putIfAbsent("globalParams", "[]");
        params.putIfAbsent("timeout", "0");
        return parseResponse(doPutForm(url, params), RESP_VOID);
    }

    /**
     * 删除工作流定义
     * DELETE /projects/{projectCode}/process-definition/{code}
     */
    public DolphinSchedulerResponse<Void> deleteWorkflow(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode;
        return parseResponse(doDelete(url), RESP_VOID);
    }

    /**
     * 发布（上线）工作流定义
     * POST /projects/{projectCode}/process-definition/{code}/release
     */
    public DolphinSchedulerResponse<Void> onlineWorkflow(String workflowCode, String workflowName) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode + "/release";
        Map<String, String> params = new HashMap<>();
        params.put("name", workflowName);
        params.put("releaseState", "ONLINE");
        return parseResponse(doPostForm(url, params), RESP_VOID);
    }

    /**
     * 下线工作流定义（相应的调度也会下线）
     * POST /projects/{projectCode}/process-definition/{code}/release
     */
    public DolphinSchedulerResponse<Void> offlineWorkflow(String workflowCode, String workflowName) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode + "/release";
        Map<String, String> params = new HashMap<>();
        params.put("name", workflowName);
        params.put("releaseState", "OFFLINE");
        return parseResponse(doPostForm(url, params), RESP_VOID);
    }

    //------------------------------------------------------------------------------------------------------------------
    // 2. 调度

    /**
     * 创建定时调度
     * POST /projects/{projectCode}/schedules
     */
    public DolphinSchedulerResponse<Schedule> createSchedule(String workflowCode, String scheduleJson) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules";
        Map<String, String> params = new LinkedHashMap<>();
        params.put("processDefinitionCode", workflowCode);
        params.put("schedule", scheduleJson);
        params.put("warningType", "NONE");
        params.put("warningGroupId", "0");
        params.put("failureStrategy", "CONTINUE");
        params.put("processInstancePriority", "MEDIUM");
        return parseResponse(doPostForm(url, params), RESP_SCHEDULE);
    }

    /**
     * 查询所有调度
     * POST /projects/{projectCode}/schedules/list
     */
    public DolphinSchedulerResponse<List<Schedule>> listSchedules() throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/list";
        return parseResponse(doPostForm(url, new HashMap<>()), RESP_SCHEDULE_LIST);
    }

    /**
     * 上线调度
     * POST /projects/{projectCode}/schedules/{id}/online
     */
    public DolphinSchedulerResponse<Void> onlineSchedule(String scheduleId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/" + scheduleId + "/online";
        return parseResponse(doPostForm(url, new HashMap<>()), RESP_VOID);
    }

    /**
     * 下线调度
     * POST /projects/{projectCode}/schedules/{id}/offline
     */
    public DolphinSchedulerResponse<Void> offlineSchedule(String scheduleId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/" + scheduleId + "/offline";
        return parseResponse(doPostForm(url, new HashMap<>()), RESP_VOID);
    }

    /**
     * 删除调度
     * DELETE /projects/{projectCode}/schedules/{id}
     */
    public DolphinSchedulerResponse<Void> deleteSchedule(String scheduleId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/" + scheduleId;
        return parseResponse(doDelete(url), RESP_VOID);
    }

    //------------------------------------------------------------------------------------------------------------------
    // 3. 工作流实例（Process Instance）

    /**
     * 查询工作流实例列表
     * GET /projects/{projectCode}/process-instances?pageNo=1&pageSize=10&searchVal=&stateType=
     */
    public DolphinSchedulerResponse<PageInfo<ProcessInstance>> listProcessInstances(int pageNo, int pageSize, String searchVal, String stateType) throws IOException {
        StringBuilder url = new StringBuilder(apiUrl + "/projects/" + projectCode + "/process-instances");
        url.append("?pageNo=").append(pageNo);
        url.append("&pageSize=").append(pageSize);
        if (searchVal != null && !searchVal.isEmpty()) {
            url.append("&searchVal=").append(searchVal);
        }
        if (stateType != null && !stateType.isEmpty()) {
            url.append("&stateType=").append(stateType);
        }
        return parseResponse(doGet(url.toString()), RESP_PAGE_PROCESS_INST);
    }

    /**
     * 查询工作流实例详情
     * GET /projects/{projectCode}/process-instances/{id}
     */
    public DolphinSchedulerResponse<ProcessInstance> processInstanceDetail(int instanceId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-instances/" + instanceId;
        return parseResponse(doGet(url), RESP_PROCESS_INST);
    }

    /**
     * 操作工作流实例（停止、恢复、暂停等）
     * POST /projects/{projectCode}/executors/execute
     * executeType: STOP / RECOVER_TOLERANCE / RECOVER_SUSPEND / PAUSE 等
     */
    public DolphinSchedulerResponse<Void> executeProcessInstance(int instanceId, String executeType) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/executors/execute";
        Map<String, String> params = new LinkedHashMap<>();
        params.put("processInstanceId", String.valueOf(instanceId));
        params.put("executeType", executeType);
        return parseResponse(doPostForm(url, params), RESP_VOID);
    }

    /**
     * 删除工作流实例
     * DELETE /projects/{projectCode}/process-instances/{id}
     */
    public DolphinSchedulerResponse<Void> deleteProcessInstance(int instanceId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-instances/" + instanceId;
        return parseResponse(doDelete(url), RESP_VOID);
    }

    //------------------------------------------------------------------------------------------------------------------
    // 4. Task & 其他

    /**
     * 生成 Task Code
     * GET /projects/{projectCode}/task-definition/gen-task-codes?genNum=1
     * @return 生成的 task code 列表
     */
    public DolphinSchedulerResponse<List<Long>> genTaskCodes(int genNum) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/task-definition/gen-task-codes?genNum=" + genNum;
        return parseResponse(doGet(url), RESP_LONG_LIST);
    }

    // Task TODO
    public DolphinSchedulerResponse<Void> createTask(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/task-definition";
        Map<String, String> params = new HashMap<>();
        params.put("taskDefinitionJson", "");
        return parseResponse(doPostForm(url, params), RESP_VOID);
    }

    /**
     * 测试连通性
     */
    public DolphinSchedulerResponse<Map<String, Object>> testConnection() throws IOException {
        String baseUrl = apiUrl.replaceAll("/dolphinscheduler.*", "");
        String healthUrl = baseUrl + "/dolphinscheduler/actuator/health";
        return parseResponse(doGetNoAuth(healthUrl), RESP_MAP);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HTTP 方法

    private String doGet(String url) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            request.setHeader("token", token);
            request.setHeader("Accept", "application/json");
            HttpResponse response = client.execute(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    private String doGetNoAuth(String url) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            request.setHeader("Accept", "application/json");
            HttpResponse response = client.execute(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    /** 发送 POST 请求，参数以表单形式（application/x-www-form-urlencoded）传递 */
    private String doPostForm(String url, Map<String, String> params) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("token", token);
            request.setHeader("Accept", "application/json");

            List<NameValuePair> formParams = new ArrayList<>();
            if (params != null) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    if (entry.getValue() != null) {
                        formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                    }
                }
            }
            if (!formParams.isEmpty()) {
                request.setEntity(new UrlEncodedFormEntity(formParams, "UTF-8"));
            }
            HttpResponse response = client.execute(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    /** 发送 PUT 请求，参数以表单形式传递 */
    private String doPutForm(String url, Map<String, String> params) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPut request = new HttpPut(url);
            request.setHeader("token", token);
            request.setHeader("Accept", "application/json");

            List<NameValuePair> formParams = new ArrayList<>();
            if (params != null) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    if (entry.getValue() != null) {
                        formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                    }
                }
            }
            if (!formParams.isEmpty()) {
                request.setEntity(new UrlEncodedFormEntity(formParams, "UTF-8"));
            }
            HttpResponse response = client.execute(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    private String doDelete(String url) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpDelete request = new HttpDelete(url);
            request.setHeader("token", token);
            request.setHeader("Accept", "application/json");
            HttpResponse response = client.execute(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    private String getString(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v == null ? null : String.valueOf(v);
    }
}
