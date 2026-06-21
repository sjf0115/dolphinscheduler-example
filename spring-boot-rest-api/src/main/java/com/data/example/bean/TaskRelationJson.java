package com.data.example.bean;

import lombok.Builder;
import lombok.Data;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:28
 */
@Data
@Builder
public class TaskRelationJson {
    private String name;
    private Integer preTaskVersion;
    private Long preTaskCode; // 0 表示无前置
    private Integer postTaskVersion;
    private Long postTaskCode;
    private String conditionType; // NONE / JUDGE
    private String conditionParams;
}
