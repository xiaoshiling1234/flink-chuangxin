package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.api.PatentDetailLawPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.MysqlUtil;
import com.chuangxin.util.ObjectUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

public class PatentDetailLaw {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HttpSourceFunction sourceFunction = getSourceFunction();


        DataStreamSource<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction);

        SingleOutputStreamOperator<String> recordsStream = streamSource.flatMap(new FlatMapFunction<Tuple2<Map<String, String>, String>, String>() {
            @Override
            public void flatMap(Tuple2<Map<String, String>, String> tuple2, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(tuple2.f1);
//                System.out.println(jsonObject);
                JSONArray records = jsonObject.getJSONObject("context").getJSONArray("records");
                JSONArray recordsJSONArray = records.getJSONArray(0);
                if (recordsJSONArray != null) {
                    for (int i = 0; i < recordsJSONArray.size(); i++) {
                        JSONObject record = recordsJSONArray.getJSONObject(i);
                        record.putIfAbsent("pid", tuple2.f0.get("pid"));
                        collector.collect(record.toString());
                    }
                }
            }
        });

        SingleOutputStreamOperator<Document> documents = recordsStream.map(Document::parse);

        //更新子任务状态
        documents.addSink(
                JdbcSink.sink(
                        "update sub_task set legal_detail_status = 1 where pid = ?",
                        (statement, document) -> {
                            String pid = document.getString("pid");
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

        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, "FLINK-SYNC:PATENT_DETAIL_LAW"));
        env.execute("FLINK-SYNC:PATENT_DETAIL_LAW");
    }

    private static HttpSourceFunction getSourceFunction() {
        return new HttpSourceFunction(GlobalConfig.API_BASH_URL + "/api/patent/detail/law") {
            @Override
            public List<Map<String, String>> getRequestParametersList() {
                List<Map<String, Object>> subTasks = MysqlUtil.query("SELECT  * from sub_task st where legal_detail_status =0");

                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                PatentDetailLawPO patentDetailLawPO = new PatentDetailLawPO();
                subTasks.forEach(task -> {
                    patentDetailLawPO.setPid((String) task.get("pid"));
                    try {
                        requestJsonList.add(ObjectUtil.objectToMap(patentDetailLawPO));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
                return requestJsonList;
            }
        };
    }

}


