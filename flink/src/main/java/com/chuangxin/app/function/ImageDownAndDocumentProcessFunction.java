package com.chuangxin.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.sync.api.BaseExpressionContext;
import com.chuangxin.bean.ImageDownBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bson.Document;

/**
 * 将字符串转换为BSON Document对象输出到主流，提取json的图片地址
 */
public class ImageDownAndDocumentProcessFunction extends ProcessFunction<String, Document> {
    BaseExpressionContext context;
    OutputTag<ImageDownBean> outputTag;
    ImageDownBean imageDownBean;

    @Override
    public void processElement(String s, ProcessFunction<String, Document>.Context context, Collector<Document> collector) {
        JSONObject jsonObject = JSON.parseObject(s);
        imageDownBean.setKeyValue(jsonObject.getString("pid"));
        imageDownBean.setImageUrl(jsonObject.getString(imageDownBean.getImageFieldName()));
        // 测输出流发送下游图片下载任务信息
        context.output(outputTag, imageDownBean);
        collector.collect(Document.parse(s));
    }

    public ImageDownAndDocumentProcessFunction(BaseExpressionContext context, OutputTag<ImageDownBean> outputTag, ImageDownBean imageDownBean) {
        this.context = context;
        this.outputTag = outputTag;
        this.imageDownBean = imageDownBean;
    }
}
