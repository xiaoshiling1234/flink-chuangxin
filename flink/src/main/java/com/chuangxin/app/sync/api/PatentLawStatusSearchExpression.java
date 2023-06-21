package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.app.function.PatentLawStatusSearchExpressionRichFlatMapFunction;
import com.chuangxin.bean.api.PatentLawStatusSearchExpressionPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.HttpClientUtils;
import com.chuangxin.util.MysqlUtil;
import com.chuangxin.util.ObjectUtil;
import com.squareup.okhttp.Response;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PatentLawStatusSearchExpression {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String maxIlsad = parameterTool.get("max_ilsad");
        //优先从参数获取，然后查库，然后使用默认参数
        if (maxIlsad != null) {
        } else if (getMaxIlsad() != null) {
            maxIlsad = getMaxIlsad();
        } else {
            maxIlsad = GlobalConfig.API_DEFAULT_ILSAD;
        }
        System.out.println("当前法律公告日日:" + maxIlsad);

        HttpSourceFunction sourceFunction = getSourceFunction(maxIlsad);
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        //为了使用状态增加虚拟keyby
        KeyedStream<String, Object> keyedStream = streamSource.keyBy((KeySelector<String, Object>) value -> "dummyKey");
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new PatentLawStatusSearchExpressionRichFlatMapFunction());

        DataStream<Document> documents = recordsStream.map((MapFunction<String, Document>) Document::parse);
        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, "FLINK-SYNC:PATENT_LAW_STATUS_SEARCH_EXPRESSION"));
        env.execute("FLINK-SYNC:PATENT_LAW_STATUS_SEARCH_EXPRESSION");
    }

    private static String getMaxIlsad() {
        List<Map<String, Object>> query = MysqlUtil.query("SELECT * from task");
        try {
            return query.get(0).get("max_ilsad").toString();
        }catch (Exception e){
            return null;
        }
    }

    private static HttpSourceFunction getSourceFunction(String maxIlsad) {
        return new HttpSourceFunction(GlobalConfig.API_BASH_URL + "/api/patent/lawStatusSearch/expression") {
            @Override
            public List<Map<String, String>> getRequestParametersList() throws IllegalAccessException, IOException {
                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                PatentLawStatusSearchExpressionPO patentLawStatusSearchExpressionPO = new PatentLawStatusSearchExpressionPO();

                int pageCount = getPageCountAndUpdateExpression(url, patentLawStatusSearchExpressionPO, maxIlsad);
                System.out.println("分页数为:" + pageCount);
                for (int i = 1; i <= pageCount; i++) {
                    patentLawStatusSearchExpressionPO.setPage(String.valueOf(i));
                    requestJsonList.add(ObjectUtil.objectToMap(patentLawStatusSearchExpressionPO));
                }
                return requestJsonList;
            }
        };
    }

    public static int getPageCountAndUpdateExpression(String url, PatentLawStatusSearchExpressionPO patentLawStatusSearchExpressionPO, String maxDt) throws IllegalAccessException, IOException {
        //这个接口需要增量拉取，所以增加时间筛选
        //使用大于等于预防一次拉不玩的情况
        String express = String.format(patentLawStatusSearchExpressionPO.getExpress() + " AND (法律公告日>%s)", maxDt);
        patentLawStatusSearchExpressionPO.setExpress(express);
        Response response = HttpClientUtils.doGet(url, ObjectUtil.objectToMap(patentLawStatusSearchExpressionPO));
        String responseData = response.body().string();
        JSONObject jsonObject = JSON.parseObject(responseData);
        if (Objects.equals(jsonObject.getString("total"), "")) {
            return 0;
        }
        int pageRow = Integer.parseInt(jsonObject.getString("page_row"));
        int total = Integer.parseInt(jsonObject.getString("total"));
        System.out.println("数据总条数:" + total);
        return total / pageRow + 1;
    }

} 


