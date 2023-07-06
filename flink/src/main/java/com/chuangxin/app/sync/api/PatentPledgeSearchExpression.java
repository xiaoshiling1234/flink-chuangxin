package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.BaseExpressionRichFlatMapFunction;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.api.BasePageExpressPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.MyKafkaUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public class PatentPledgeSearchExpression {
    public static void main(String[] args) throws Exception {
        BaseExpressionContext context = new BaseExpressionContext("FLINK-SYNC:PATENT_PLEDGE_SEARCH_EXPRESSION");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.out.printf("当前%s:%s%n", context.incCn, context.maxDt);
        BasePageExpressPO basePageExpressPO = new BasePageExpressPO(context.incCol);
        HttpSourceFunction sourceFunction = context.getHttpPageSourceFunction("/api/patent/pledgeSearch/expression", basePageExpressPO);
        DataStreamSource<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction);
        KeyedStream<String, Object> keyedStream = streamSource.map(x -> x.f1).keyBy((KeySelector<String, Object>) value -> "dummyKey");
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new BaseExpressionRichFlatMapFunction(context));
        OutputTag<String> outputTag = new OutputTag<String>("ImageUrl") {
        };

        SingleOutputStreamOperator<Document> documents = recordsStream.process(new ProcessFunction<String, Document>() {
            final HashMap<String, String> map = new HashMap<>();
            @Override
            public void processElement(String value, ProcessFunction<String, Document>.Context ctx, Collector<Document> out) {
                JSONObject jsonObject = JSON.parseObject(value);
                map.clear();
                map.put("task_name", context.getTaskName());
                map.put("pid", jsonObject.getString("pid"));
                map.put("field_name", "IMGO");
                map.put("imageUrl", jsonObject.getString("IMGO"));
                String json = JSON.toJSONString(map);
                ctx.output(outputTag, json);
                out.collect(Document.parse(value));
            }
        });

        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, context.taskName));

//        documents.getSideOutput(outputTag).print("ImageUrl>>>>>>>>>>>");

        documents.getSideOutput(outputTag).addSink(MyKafkaUtil.getKafkaProducer(GlobalConfig.KAFKA_IMAGE_SOURCE_TOPIC));
        env.execute(context.taskName);
    }
} 


