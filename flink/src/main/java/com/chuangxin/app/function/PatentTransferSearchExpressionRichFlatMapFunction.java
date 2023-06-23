package com.chuangxin.app.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.util.MysqlUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;

import static com.chuangxin.util.DateTimeUtil.convertDateFormat;

public class PatentTransferSearchExpressionRichFlatMapFunction extends RichFlatMapFunction<String, String> {
    private ValueState<String> maxDtState;

    @Override
    public void open(Configuration parameters) {
        // 初始化状态变量
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
                "maxDtState",
                String.class
        );
        maxDtState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void close() {
        try {
            HashMap<String, Object> updateInfo = new HashMap<>();
            updateInfo.put("max_dt", maxDtState.value());
            MysqlUtil.update("task", updateInfo, "task_name='FLINK-SYNC:PATENT_TRANSFER_SEARCH_EXPRESSION'");
        } catch (Exception ignored) {
        }
        System.out.println("任务结束");
    }

    @Override
    public void flatMap(String s, Collector<String> collector) {
        JSONObject jsonObject = JSONObject.parseObject(s);
        JSONArray records = jsonObject.getJSONObject("context").getJSONArray("records");
        records.forEach(record -> {
            String pd = JSONObject.parseObject(record.toString()).getString("pd");
            String pdFormat = convertDateFormat(pd);

            try {
                if (maxDtState.value() == null) {
                    maxDtState.update(pdFormat);
                    System.out.println("最大发布时间已更新为:" + pdFormat);
                } else {
                    String currentMaxDt = maxDtState.value();
                    if (pdFormat.compareTo(currentMaxDt) > 0) {
                        maxDtState.update(pdFormat);
                        System.out.println("最大发布时间已更新为:" + pdFormat);
                    }
                }
                collector.collect(record.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
