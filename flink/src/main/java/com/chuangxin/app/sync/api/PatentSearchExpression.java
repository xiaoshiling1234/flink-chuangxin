package com.chuangxin.app.sync.api;

import com.chuangxin.app.function.BaseExpressionRichFlatMapFunction;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.ImageDownAndDocumentProcessFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.ImageDownBean;
import com.chuangxin.bean.api.PatentSearchExpressionPO;
import com.chuangxin.common.GlobalConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.bson.Document;

import java.util.Map;

public class PatentSearchExpression {
    public static void main(String[] args) throws Exception {
        BaseExpressionContext context = new BaseExpressionContext("FLINK-SYNC:PATENT_SEARCH_EXPRESSION");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.printf("当前%s:%s%n", context.incCn, context.maxDt);
        PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
        patentSearchExpressionPO.setSort_column("+" + context.incCol.toUpperCase());
        HttpSourceFunction sourceFunction = context.getHttpPageSourceFunction("/api/patent/search/expression", patentSearchExpressionPO);
        DataStream<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction).rebalance();
        KeyedStream<String, Object> keyedStream = streamSource.map(x -> x.f1).keyBy((KeySelector<String, Object>) value -> "dummyKey");

        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new BaseExpressionRichFlatMapFunction(context));
        OutputTag<ImageDownBean> outputTag = new OutputTag<ImageDownBean>("ImageInfo") {
        };
        ImageDownBean imageDownBean = new ImageDownBean(context.getTaskName(), "ano", "IMGO");
        SingleOutputStreamOperator<Document> documents = recordsStream.rebalance().process(new ImageDownAndDocumentProcessFunction(context, outputTag, imageDownBean));
        // 写入 MongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, context.taskName)).name("MongoDB Sink");

        // 下载图片，并存储下载任务信息
        context.processImage(outputTag, documents);

        // 写入子任务
        writeSubTask(documents);

        env.execute(context.taskName);
    }

    private static void writeSubTask(SingleOutputStreamOperator<Document> documents) {
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
        ).name("JDBC Sink");
    }


}
