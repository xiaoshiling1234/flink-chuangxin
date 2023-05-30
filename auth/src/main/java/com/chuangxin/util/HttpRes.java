package com.chuangxin.util;

import com.chuangxin.entity.enums.ResEnum;
import lombok.Data;

@Data
public class HttpRes {
    
    private String code;
    private String message;
    private Object data;

    public HttpRes(ResEnum status, Object data, String message) {
        this.code = status.getCode();
        this.message = message;
        this.data = data;
    }

    public HttpRes(ResEnum status, Object data) {
        this.code = status.getCode();
        this.message = status.getDetail();
        this.data = data;
    }

    public HttpRes(ResEnum status) {
        this.code = status.getDetail();
        this.message = status.getDetail();
    }
}
