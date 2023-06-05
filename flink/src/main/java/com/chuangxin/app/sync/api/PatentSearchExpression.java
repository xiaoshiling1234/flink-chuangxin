package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.api.PatentSearchExpressionPO;
import com.chuangxin.bean.api.RangePO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.HttpClientUtils;
import com.squareup.okhttp.Response;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PatentSearchExpression {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        RangePO rangePO = new RangePO(parameterTool);
        HttpSourceFunction sourceFunction = getSourceFunction(rangePO);
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        DataStream<Document> documents = streamSource.map((MapFunction<String, Document>) Document::parse);
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, "patent_search_expression"));
        env.execute("FLINK-SYNC:PATENT_SEARCH_EXPRESSION");
    }

    private static HttpSourceFunction getSourceFunction(RangePO rangePO) {
        return new HttpSourceFunction(GlobalConfig.BASH_URL + "/api/patent/search/expression") {
            @Override
            public List<Map<String, String>> getRequestParametersList() throws IllegalAccessException {
                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
                try {
                    int pageCount = getPageCount(url, patentSearchExpressionPO, rangePO.getBeginDate(), rangePO.getEndDate());
                    //更新结束页码,默认设置的一个很大的值
                    rangePO.setEndPage(Math.min(pageCount, rangePO.getEndPage()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                for (int i = rangePO.getStartPage(); i <= rangePO.getEndPage(); i++) {
                    patentSearchExpressionPO.setPage(String.valueOf(i));
                    requestJsonList.add(HttpClientUtils.objectToMap(patentSearchExpressionPO));
                }
                return requestJsonList;
            }
        };
    }

    public static int getPageCount(String url, PatentSearchExpressionPO patentSearchExpressionPO, String beginDate, String endDate) throws IllegalAccessException, IOException {
        //这个接口需要增量拉取，所以增加时间筛选
        String newExpress = String.format(patentSearchExpressionPO.getExpress() + " AND (公布日=(%s TO %s))", beginDate, endDate);
        patentSearchExpressionPO.setExpress(newExpress);
        Response response = HttpClientUtils.doGet(url, HttpClientUtils.objectToMap(patentSearchExpressionPO));
        String responseData = response.body().string();
        JSONObject jsonObject = JSON.parseObject(responseData);
        int pageRow = Integer.parseInt(jsonObject.getString("page_row"));
        int total = Integer.parseInt(jsonObject.getString("total"));
        System.out.println(total);
        return total / pageRow + 1;
    }
} 


