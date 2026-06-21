package com.data.example.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

    private static final Gson gson = new GsonBuilder().create();

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
    
    /**
     * 创建工作流定义。
     * POST /projects/{projectCode}/process-definition
     */
    public String createWorkflow(Map<String, String> params) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition";
        // 补充必填参数
        params.putIfAbsent("tenantCode", tenantCode);
        params.putIfAbsent("locations", "[]");
        params.putIfAbsent("globalParams", "[]");
        params.putIfAbsent("timeout", "0");
        return doPostForm(url, params);
    }

    /**
     * 更新工作流定义。
     * PUT /projects/{projectCode}/process-definition/{code}
     */
    public String updateWorkflow(String workflowCode, Map<String, String> params) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode;
        params.putIfAbsent("tenantCode", tenantCode);
        params.putIfAbsent("locations", "[]");
        params.putIfAbsent("globalParams", "[]");
        params.putIfAbsent("timeout", "0");
        return doPutForm(url, params);
    }

    /**
     * 删除工作流定义。
     * DELETE /projects/{projectCode}/process-definition/{code}
     */
    public String deleteWorkflow(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode;
        return doDelete(url);
    }

    /**
     * 发布（上线）工作流定义。
     * POST /projects/{projectCode}/process-definition/{code}/release
     */
    public String onlineWorkflow(String workflowCode, String workflowName) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode + "/release";
        Map<String, String> params = new HashMap<>();
        params.put("name", workflowName);
        params.put("releaseState", "ONLINE");
        return doPostForm(url, params);
    }

    /**
     * 下线工作流定义。
     * POST /projects/{projectCode}/process-definition/{code}/release
     */
    public String offlineWorkflow(String workflowCode, String workflowName) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode + "/release";
        Map<String, String> params = new HashMap<>();
        params.put("name", workflowName);
        params.put("releaseState", "OFFLINE");
        return doPostForm(url, params);
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 创建定时调度。
     * POST /projects/{projectCode}/schedules
     * 参数：processDefinitionCode, schedule (cron JSON)
     */
    public String createSchedule(String workflowCode, String scheduleJson) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules";
        Map<String, String> params = new java.util.LinkedHashMap<>();
        // 必填参数
        params.put("processDefinitionCode", workflowCode);
        params.put("schedule", scheduleJson);
        params.put("warningType", "NONE");
        params.put("warningGroupId", "0");
        params.put("failureStrategy", "CONTINUE");
        params.put("processInstancePriority", "MEDIUM");
        return doPostForm(url, params);
    }

    /**
     * 所有调度。
     * POST /projects/{projectCode}/schedules/list
     * 参数：processDefinitionCode, schedule (cron JSON)
     */
    public String listSchedules() throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/list";
        return doPostForm(url, new HashMap<>());
    }

    /**
     * 上线调度。
     * POST /projects/{projectCode}/schedules/{id}/online
     */
    public String onlineSchedule(String scheduleId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/" + scheduleId + "/online";
        return doPostForm(url, new HashMap<>());
    }

    /**
     * 下线调度。
     * POST /projects/{projectCode}/schedules/{id}/offline
     */
    public String offlineSchedule(String scheduleId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/" + scheduleId + "/offline";
        return doPostForm(url, new HashMap<>());
    }

    /**
     * 删除调度
     * POST /projects/{projectCode}/schedules/{id}/offline
     */
    public String deleteSchedule(String scheduleId) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/schedules/" + scheduleId;
        return doDelete(url);
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 手动触发工作流执行。
     * POST /projects/{projectCode}/executors/start-process-instance
     */
    public String triggerWorkflow(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/executors/start-process-instance";
        Map<String, String> params = new java.util.LinkedHashMap<>();
        params.put("processDefinitionCode", workflowCode);
        params.put("scheduleTime", "[]");
        params.put("failureStrategy", "CONTINUE");
        params.put("warningType", "NONE");
        params.put("warningGroupId", "0");
        params.put("processInstancePriority", "MEDIUM");
        params.put("runMode", "RUN_MODE_SERIAL");
        return doPostForm(url, params);
    }

    /** 查询工作流 */
    public String getWorkflow(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/process-definition/" + workflowCode;
        return doGet(url);
    }

    /**
     * 生成 Task Code。
     * GET /projects/{projectCode}/task-definition/gen-task-codes?genNum=1
     * @return 生成的 task code 列表
     */
    public String genTaskCodes(int genNum) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/task-definition/gen-task-codes?genNum=" + genNum;
        return doGet(url);
    }

    // Task
    // TODO
    public String createTask(String workflowCode) throws IOException {
        String url = apiUrl + "/projects/" + projectCode + "/task-definition";
        Map<String, String> params = new HashMap<>();
        params.put("taskDefinitionJson", "");
        return doPostForm(url, params);
    }

    /**
     * 测试连通性。
     */
    public String testConnection() throws IOException {
        String baseUrl = apiUrl.replaceAll("/dolphinscheduler.*", "");
        String healthUrl = baseUrl + "/dolphinscheduler/actuator/health";
        return doGetNoAuth(healthUrl);
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
