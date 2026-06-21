package com.data.example.bean;

import lombok.Data;

import java.util.List;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/6/14 16:28
 */
@Data
public class PageInfo<T> {
    private Integer total;
    private List<T> totalList;
}
