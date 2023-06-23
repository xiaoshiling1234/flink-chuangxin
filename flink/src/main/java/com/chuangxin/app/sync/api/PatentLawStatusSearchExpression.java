package com.chuangxin.app.sync.api;

import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.app.function.PatentLawStatusSearchExpressionRichFlatMapFunction;
import com.chuangxin.bean.api.PatentLawStatusPO;
import com.chuangxin.common.GlobalConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

import java.util.Map;

public class PatentLawStatusSearchExpression {
    public static void main(String[] args) throws Exception {
        BaseExpressionContext context = new BaseExpressionContext("FLINK-SYNC:PATENT_LAW_STATUS_SEARCH_EXPRESSION");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.out.println("当前发布日:" + context.maxDt);
        PatentLawStatusPO patentLawStatusPO = new PatentLawStatusPO();
        patentLawStatusPO.setSort_column("+PD");
        HttpSourceFunction sourceFunction = context.getHttpPageSourceFunction("/api/patent/lawStatusSearch/expression", patentLawStatusPO);
        DataStreamSource<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction);
        KeyedStream<String, Object> keyedStream = streamSource.map(x -> x.f1).keyBy((KeySelector<String, Object>) value -> "dummyKey");
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new PatentLawStatusSearchExpressionRichFlatMapFunction());

        DataStream<Document> documents = recordsStream.map((MapFunction<String, Document>) Document::parse);
        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, context.taskName));
        env.execute(context.taskName);
    }
} 


