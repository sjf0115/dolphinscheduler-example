package com.data.example.config;

import com.data.example.client.DolphinSchedulerApiClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 功能：DolphinScheduler 配置类
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:14
 */
@Data
@Slf4j
@Configuration
@ConfigurationProperties(prefix = "dolphinscheduler.api")
public class DolphinSchedulerConfig {
    private String baseUrl;
    private String token;
    private String projectCode;
    private String tenantCode = "default";
    private int connectTimeout = 5000;
    private int readTimeout = 10000;
    private int maxTotalConnections = 200;
    private int maxPerRoute = 50;

    /**
     * 将 DolphinSchedulerApiClient 注册为 Spring Bean，
     * 启动时自动从 application.yml 读取配置并初始化
     */
    @Bean
    public DolphinSchedulerApiClient dolphinSchedulerApiClient() {
        DolphinSchedulerApiClient client = new DolphinSchedulerApiClient();
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("apiUrl", baseUrl);
        config.put("token", token);
        config.put("projectCode", projectCode);
        config.put("tenantCode", tenantCode);
        client.init(config);
        log.info("DolphinSchedulerApiClient 初始化完成: baseUrl={}, projectCode={}", baseUrl, projectCode);
        return client;
    }
}
