package com.chuangxin.app.sync.api;

import com.chuangxin.app.function.BaseExpressionRichFlatMapFunction;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.ImageDownAndDocumentProcessFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.ImageDownBean;
import com.chuangxin.bean.api.BasePageExpressPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.bson.Document;

import java.util.Map;

public class PatentLawStatusSearchExpression {
    public static void main(String[] args) throws Exception {
        BaseExpressionContext context = new BaseExpressionContext("FLINK-SYNC:PATENT_LAW_STATUS_SEARCH_EXPRESSION");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.printf("当前%s:%s%n", context.incCn, context.maxDt);
        BasePageExpressPO basePageExpressPO = new BasePageExpressPO(context.incCol);
        HttpSourceFunction sourceFunction = context.getHttpPageSourceFunction("/api/patent/lawStatusSearch/expression", basePageExpressPO);
        DataStream<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction).rebalance();
        KeyedStream<String, Object> keyedStream = streamSource.map(x -> x.f1).keyBy((KeySelector<String, Object>) value -> "dummyKey");
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new BaseExpressionRichFlatMapFunction(context));

        OutputTag<String> outputTag = new OutputTag<String>("ImageUrl") {
        };
        ImageDownBean imageDownBean = new ImageDownBean(context.getTaskName(), "ano", "IMGO");
        SingleOutputStreamOperator<Document> documents = recordsStream.process(new ImageDownAndDocumentProcessFunction(context, outputTag, imageDownBean));
        // 写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, context.taskName));
        // 图片下载任务写入Kafka
        documents.getSideOutput(outputTag).rebalance().addSink(MyKafkaUtil.getKafkaProducer(GlobalConfig.KAFKA_IMAGE_SOURCE_TOPIC));
        env.execute(context.taskName);
    }
} 


