package com.data.example.domain;

public enum ResponseCode {
    // 请求成功
    SUCCESS(0, "success"),
    // 请求失败
    ERROR(1, "error"),
    ;

    private Integer code;
    private String message;

    ResponseCode(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    public Integer getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}