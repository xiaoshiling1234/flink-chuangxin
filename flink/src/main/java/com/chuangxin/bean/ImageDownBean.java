package com.chuangxin.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ImageDownBean implements Serializable {
    /**
     * 任务名称
     */
    String taskName;
    /**
     * 主键字段
     */
    String keyField;
    /**
     * 主键字段值
     */
    String keyValue;
    /**
     * 图片字段
     */
    String imageFieldName;
    /**
     * 图片地址
     */
    String imageUrl;

    public ImageDownBean(String taskName, String keyField,String imageFieldName) {
        this.taskName = taskName;
        this.keyField = keyField;
        this.imageFieldName=imageFieldName;
    }
}
