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

/**
 * "ilsad": "2005/12/02 00:00:00;2006/11/15 00:00:00;2006/12/15 00:00:00;2006/01/18 00:00:00;2006/12/28 00:00:00;2006/12/29 00:00:00;2007/01/31 00:00:00;2007/02/15 00:00:00;2007/02/20 00:00:00;2007/04/16 00:00:00;2007/05/01 00:00:00;2007/05/25 00:00:00;2007/06/16 00:00:00;2007/06/27 00:00:00;2007/09/21 00:00:00;2007/10/24 00:00:00;2008/04/30 00:00:00;2008/05/30 00:00:00;2008/09/30 00:00:00;2009/01/30 00:00:00;2009/02/27 00:00:00;2009/08/31 00:00:00;2009/09/30 00:00:00;2013/08/15 00:00:00;2015/10/14 00:00:00;2016/07/29 00:00:00;2016/10/18 00:00:00;2017/10/26 00:00:00;2018/10/26 00:00:00;2022/12/30 00:00:00;2006/04/13 00:00:00;2023/01/31 00:00:00;2023/02/28 00:00:00;2006/06/22 00:00:00;2006/09/20 00:00:00;2006/10/13 00:00:00"
 * 需要截取保留最大的
 */
public class PatentLawStatusSearchExpressionRichFlatMapFunction extends RichFlatMapFunction<String, String> {
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
            updateInfo.put("max_ilsad", maxDtState.value());
            MysqlUtil.update("task", updateInfo, "1=1");
        } catch (Exception ignored) {
        }
        System.out.println("任务结束");
    }

    @Override
    public void flatMap(String s, Collector<String> collector) {
        JSONObject jsonObject = JSONObject.parseObject(s);
        JSONArray records = jsonObject.getJSONObject("context").getJSONArray("records");
        records.forEach(record -> {
            String ilsadHistory = JSONObject.parseObject(record.toString()).getString("ilsad");
            String[] split = ilsadHistory.split(";");
            String ilsad="";
            for (String s1 : split) {
                if (ilsad.equals("")){
                    ilsad=s1;
                }else {
                    if (s1.compareTo(ilsad)>0){
                        ilsad=s1;
                    }
                }
            }

            String ilsadFormat = convertDateFormat(ilsad);

            try {
                if (maxDtState.value() == null) {
                    maxDtState.update(ilsadFormat);
                    System.out.println("最大法律公告日已更新为:" + ilsadFormat);
                } else {
                    String currentMaxDt = maxDtState.value();
                    if (ilsadFormat.compareTo(currentMaxDt) > 0) {
                        maxDtState.update(ilsadFormat);
                        System.out.println("最大法律公告日已更新为:" + ilsadFormat);
                    }
                }
                collector.collect(record.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
