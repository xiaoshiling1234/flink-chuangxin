package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.bean.api.PatentSearchExpressionPO;
import com.chuangxin.util.DateTimeUtil;
import com.chuangxin.util.HttpClientUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PatentSearchExpression {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String beginDate = parameterTool.get("begin_date", DateTimeUtil.getYesterdayYMD());
        String endDate = parameterTool.get("end_date", DateTimeUtil.getYesterdayYMD());
        Integer startPage = parameterTool.getInt("start_page", 1);
        Integer endPage = parameterTool.getInt("end_page", -1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.addSource(new HttpSourceFunction("http://114.251.8.193/api/patent/search/expression") {
            @Override
            public List<String> getRequestJsonList() {
                // todo://请求失败和页数都需要保存在mysql里面
                ArrayList<String> requestJsonList = new ArrayList<>();
                // 同一天,根据end_page决定要不要先请求一次接口获取总页数
                if (Objects.equals(beginDate, endDate) && endPage == -1) {
                    PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
                    //这个会返回总页数，并且会更新表达式的日期限制
                    int pageCount = getPageCount(url, patentSearchExpressionPO, beginDate);
                    for (int i = startPage; i <= pageCount; i++) {
                        patentSearchExpressionPO.setPage(String.valueOf(i));
                        requestJsonList.add(JSON.toJSONString(patentSearchExpressionPO));
                    }
                } else if (Objects.equals(beginDate, endDate) && endPage >= startPage) {
                    PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
                    //这个会返回总页数，并且会更新表达式的日期限制
                    int pageCount = getPageCount(url, patentSearchExpressionPO, beginDate);
                    //取endPage和pageCount的较小者
                    for (int i = startPage; i <= Math.min(pageCount, endPage); i++) {
                        patentSearchExpressionPO.setPage(String.valueOf(i));
                        requestJsonList.add(JSON.toJSONString(patentSearchExpressionPO));
                    }
                } else if (Integer.parseInt(endDate) > Integer.parseInt(beginDate)) {
                    //对日期循环
                    for (int i = Integer.parseInt(beginDate); i <= Integer.parseInt(endDate); i++) {
                        PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
                        //这个会返回总页数，并且会更新表达式的日期限制
                        //这个接口需要增量拉取，所以增加时间筛选
                        int pageCount = getPageCount(url, patentSearchExpressionPO, String.valueOf(i));
                        for (int j = 1; j <= pageCount; j++) {
                            patentSearchExpressionPO.setPage(String.valueOf(j));
                            requestJsonList.add(JSON.toJSONString(patentSearchExpressionPO));
                        }
                    }
                }
                System.out.println(requestJsonList);
                return requestJsonList;
            }
        });
        streamSource.print();
        env.execute();
    }

    public static int getPageCount(String url, PatentSearchExpressionPO patentSearchExpressionPO, String date) {
        //这个接口需要增量拉取，所以增加时间筛选
        String newExpress = String.format(patentSearchExpressionPO.getExpress() + " AND 公布日=%s", date);
        patentSearchExpressionPO.setExpress(newExpress);
        String result = HttpClientUtils.doPostJson(url, JSON.toJSONString(patentSearchExpressionPO));
        JSONObject jsonObject = JSON.parseObject(result);
        int pageRow = Integer.parseInt(jsonObject.getString("page_row"));
        int total = Integer.parseInt(jsonObject.getString("total"));
        return total / pageRow + 1;
    }
} 


