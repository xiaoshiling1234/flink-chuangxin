package com.chuangxin.app.sync.api;

import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.app.function.PatentTransferRecordSearchExpressionRichFlatMapFunction;
import com.chuangxin.bean.api.PatentTransferRecordSearchPO;
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


public class PatentTransferRecordSearchExpression {
    public static void main(String[] args) throws Exception {
        BaseExpressionContext context = new BaseExpressionContext("FLINK-SYNC:PATENT_TRANSFER_RECORD_SEARCH_EXPRESSION");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.out.println("授权公布日:" + context.maxDt);
        HttpSourceFunction sourceFunction = context.getHttpPageSourceFunction("/api/patent/transferRecordSearch/expression", new PatentTransferRecordSearchPO());
        DataStreamSource<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction);
        //为了使用状态增加虚拟keyby
        KeyedStream<String, Object> keyedStream = streamSource.map(x -> x.f1).keyBy((KeySelector<String, Object>) value -> "dummyKey");
        ;
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new PatentTransferRecordSearchExpressionRichFlatMapFunction());
        DataStream<Document> documents = recordsStream.map((MapFunction<String, Document>) Document::parse);
        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, context.taskName));
        env.execute(context.taskName);
    }
} 


