package com.data.example.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import com.data.example.bean.Project;
import com.data.example.domain.Response;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;

/**
 * 功能：DolphinScheduler 客户端
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/3/5 10:43
 */
public class DolphinSchedulerClient {
    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final String baseUrl = "http://localhost:12345/dolphinscheduler/projects";
    private volatile String token = "2b14d6706fcf66db5045569c071da4c9";
    private final CloseableHttpClient httpClient;

    // 初始化客户端
    public DolphinSchedulerClient() {
        // 配置连接池
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        connManager.setDefaultMaxPerRoute(20);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(5))
                .setResponseTimeout(Timeout.ofSeconds(30))
                .build();

        this.httpClient = HttpClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig)
                .evictExpiredConnections()
                .build();
    }

    // 加载配置文件
    private Properties loadConfig(String path) {
        try (InputStream input = new FileInputStream(path)) {
            Properties props = new Properties();
            props.load(input);
            return props;
        } catch (IOException e) {
            throw new DSApiException("加载配置文件失败", e);
        }
    }

    // 统一执行请求
    private <T> Response<T> executeRequest(HttpUriRequest request, Class<T> responseType) {
        try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
            String responseContent = EntityUtils.toString(httpResponse.getEntity());

            if (httpResponse.getCode() != HttpStatus.SC_OK) {
                throw new DSApiException("API请求失败: " + responseContent);
            }

            TypeReference<Response<T>> typeRef = new TypeReference<Response<T>>() {};
            Response<T> response = mapper.readValue(responseContent, typeRef);

            return response;
        } catch (IOException | ParseException e) {
            throw new DSApiException("请求执行异常", e);
        }
    }


    // 查询工作流定义
    public void getProjects() {
        try {
            URI uri = new URIBuilder(baseUrl)
                    .appendPath("list")
                    .build();

            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader("token", token);
            Response<Project> response = executeWithRetry(httpGet, Project.class, 3);
            Project project = response.getData();
        } catch (URISyntaxException e) {
            throw new DSApiException("构建请求失败", e);
        }
    }



//    // 创建工作流定义
//    public WorkflowDefinition createWorkflow(String projectName, WorkflowCreateRequest request) {
//        try {
//            URI uri = new URIBuilder(baseUrl)
//                    .appendPathSegments("projects", projectName, "process", "define")
//                    .build();
//
//            HttpPost httpPost = new HttpPost(uri);
//            httpPost.setHeader("token", token);
//            httpPost.setEntity(new StringEntity(mapper.writeValueAsString(request), ContentType.APPLICATION_JSON));
//
//            return executeRequest(httpPost, WorkflowDefinition.class);
//        } catch (URISyntaxException | JsonProcessingException e) {
//            throw new DSApiException("创建请求失败", e);
//        }
//    }

    // 启动作业实例
//    public ProcessInstance startWorkflow(String projectName, WorkflowExecuteRequest params) {
//        try {
//            URI uri = new URIBuilder(baseUrl)
//                    .appendPathSegments("projects", projectName, "executors", "start-process-instance")
//                    .build();
//
//            HttpPost httpPost = new HttpPost(uri);
//            httpPost.setHeader("token", token);
//            httpPost.setEntity(new StringEntity(mapper.writeValueAsString(params), ContentType.APPLICATION_JSON));
//
//            return executeRequest(httpPost, ProcessInstance.class);
//        } catch (URISyntaxException | JsonProcessingException e) {
//            throw new DSApiException("启动作业失败", e);
//        }
//    }

    // 查询实例状态（带自动重试）
    public InstanceStatus getInstanceStatus(String projectName, int instanceId) {
        try {
            URI uri = new URIBuilder(baseUrl)
                    .appendPathSegments("projects", projectName, "executors",
                            String.valueOf(instanceId), "active-task")
                    .build();

            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader("token", token);

            Response<InstanceStatus> response = executeWithRetry(httpGet, InstanceStatus.class, 3);
            return response.getData();
        } catch (URISyntaxException e) {
            throw new DSApiException("构建请求失败", e);
        }
    }

    // 带重试机制的请求执行
    private <T> Response<T> executeWithRetry(HttpUriRequest request, Class<T> type, int maxRetries) {
        for (int i=0; i<maxRetries; i++) {
            try {
                return executeRequest(request, type);
            } catch (DSApiException e) {
                if (e.getMessage().contains("token expired") && i < maxRetries-1) {
                    request.setHeader("token", token);
                    continue;
                }
                throw e;
            }
        }
        throw new DSApiException("重试次数耗尽");
    }

    // DTO定义
    @Data
    public static class AuthResponse {
        private String token;
    }

//    @Data
//    public static class WorkflowCreateRequest {
//        private String name;
//        private String description;
//        private List<TaskDefinition> tasks;
//    }

    @Data
    public static class WorkflowDefinition {
        private Long code;
        private String name;
        private Long id;
    }

    @Data
    public static class WorkflowExecuteRequest {
        private Long processDefinitionId;
        private String scheduleTime;
        private Map<String, String> parameters;
    }

    @Data
    public static class ProcessInstance {
        private int instanceId;
        private String state;
    }

    @Data
    public static class InstanceStatus {
        private String state;
        private String startTime;
        private String endTime;
    }

    // 自定义异常
    public static class DSApiException extends RuntimeException {
        public DSApiException(String message) { super(message); }
        public DSApiException(String message, Throwable cause) { super(message, cause); }
    }

    public static void main(String[] args) {
        DolphinSchedulerClient client = new DolphinSchedulerClient();
        client.getProjects();
    }
}
