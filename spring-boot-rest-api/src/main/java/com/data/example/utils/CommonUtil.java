package com.data.example.utils;

import com.data.example.bean.DolphinSchedulerResponse;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/21 11:31
 */
@Slf4j
public class CommonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    /**
     * 解析 DS API 返回的 JSON 为统一响应对象
     */
    public static DolphinSchedulerResponse<?> parseResponse(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, DolphinSchedulerResponse.class);
        } catch (JsonProcessingException e) {
            log.error("解析 DolphinScheduler 响应失败: {}", e.getMessage());
            return new DolphinSchedulerResponse<>(-1, "响应解析失败: " + rawJson, null, false);
        }
    }

    public static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON 序列化失败", e);
        }
    }
}
