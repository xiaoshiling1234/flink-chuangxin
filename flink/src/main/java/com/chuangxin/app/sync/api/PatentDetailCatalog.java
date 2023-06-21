package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.api.PatentDetailCatalogPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.MysqlUtil;
import com.chuangxin.util.ObjectUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PatentDetailCatalog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HttpSourceFunction sourceFunction = getSourceFunction();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        SingleOutputStreamOperator<Tuple2<String, String>> recordsStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                JSONArray records = jsonObject.getJSONObject("context").getJSONArray("records");
                for (int i = 0; i < records.size(); i++) {
                    JSONObject record = records.getJSONObject(i);
                    String pid = record.getJSONObject("catalogPatent").getString("pid");
                    collector.collect(new Tuple2<>(pid, record.toString()));
                }
            }
        });

        //更新子任务状态
        recordsStream.addSink(
                JdbcSink.sink(
                        "update sub_task set patent_detail_status = 1 where pid = ?",
                        (statement, tuble2) -> {
                            String pid = tuble2.f0;
                            statement.setString(1, pid);
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

        SingleOutputStreamOperator<Document> documents = recordsStream.map((MapFunction<Tuple2<String, String>, Document>) stringStringTuple2 -> Document.parse(stringStringTuple2.f1));
        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, "FLINK-SYNC:PATENT_DETAIL_CATALOG"));
        env.execute("FLINK-SYNC:PATENT_DETAIL_CATALOG");
    }

    private static HttpSourceFunction getSourceFunction() {
        return new HttpSourceFunction(GlobalConfig.API_BASH_URL + "/api/patent/detail/catalog") {
            @Override
            public List<Map<String, String>> getRequestParametersList() {
                List<Map<String, Object>> subTasks = MysqlUtil.query("SELECT  * from sub_task st where patent_detail_status =0");

                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                PatentDetailCatalogPO patentDetailCatalogPO = new PatentDetailCatalogPO();
                subTasks.forEach(task -> {
                    patentDetailCatalogPO.setPid((String) task.get("pid"));
                    try {
                        requestJsonList.add(ObjectUtil.objectToMap(patentDetailCatalogPO));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
                return requestJsonList;
            }
        };
    }

}


