package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.PatentSearchExpressionRichFlatMapFunction;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.api.PatentSearchExpressionPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.HttpClientUtils;
import com.chuangxin.util.MysqlUtil;
import com.chuangxin.util.ObjectUtil;
import com.squareup.okhttp.Response;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PatentSearchExpression {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String maxPD = parameterTool.get("max_pd");
        //优先从参数获取，然后查库，然后使用默认参数
        if (maxPD != null) {
        } else if (getMaxPD() != null) {
            maxPD = getMaxPD();
        } else {
            maxPD = GlobalConfig.API_DEFAULT_PD;
        }
        System.out.println("当前发布日:" + maxPD);

        HttpSourceFunction sourceFunction = getSourceFunction(maxPD);
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        //为了使用状态增加虚拟keyby
        KeyedStream<String, Object> keyedStream = streamSource.keyBy((KeySelector<String, Object>) value -> "dummyKey");
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new PatentSearchExpressionRichFlatMapFunction());

        DataStream<Document> documents = recordsStream.map((MapFunction<String, Document>) Document::parse);
        //写入子任务
        documents.addSink(
                JdbcSink.sink(
                        "insert into sub_task (pid, pno) values (?, ?)",
                        (statement, document) -> {
                            String pid = document.getString("pid");
                            String pno = document.getString("pno");
                            statement.setString(1, pid);
                            statement.setString(2, pno);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(50)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(GlobalConfig.MYSQL_URL)
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername(GlobalConfig.MYSQL_USER)
                                .withPassword(GlobalConfig.MYSQL_PASSWORD)
                                .build()
                )
        );
        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, "FLINK-SYNC:PATENT_SEARCH_EXPRESSION"));
        env.execute("FLINK-SYNC:PATENT_SEARCH_EXPRESSION");
    }

    private static String getMaxPD() {
        List<Map<String, Object>> query = MysqlUtil.query("SELECT * from task");
        return query.size() == 0 ? null : query.get(0).get("max_pd").toString();
    }

    private static HttpSourceFunction getSourceFunction(String maxDt) {
        return new HttpSourceFunction(GlobalConfig.API_BASH_URL + "/api/patent/search/expression") {
            @Override
            public List<Map<String, String>> getRequestParametersList() throws IllegalAccessException, IOException {
                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();

                int pageCount = getPageCountAndUpdateExpression(url, patentSearchExpressionPO, maxDt);
                System.out.println("分页数为:" + pageCount);
                for (int i = 1; i <= pageCount; i++) {
                    patentSearchExpressionPO.setPage(String.valueOf(i));
                    requestJsonList.add(ObjectUtil.objectToMap(patentSearchExpressionPO));
                }
                return requestJsonList;
            }
        };
    }

    public static int getPageCountAndUpdateExpression(String url, PatentSearchExpressionPO patentSearchExpressionPO, String maxDt) throws IllegalAccessException, IOException {
        //这个接口需要增量拉取，所以增加时间筛选
        //使用大于等于预防一次拉不玩的情况
        String express = String.format(patentSearchExpressionPO.getExpress() + " AND (公布日>=%s)", maxDt);
        patentSearchExpressionPO.setExpress(express);
        Response response = HttpClientUtils.doGet(url, ObjectUtil.objectToMap(patentSearchExpressionPO));
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


