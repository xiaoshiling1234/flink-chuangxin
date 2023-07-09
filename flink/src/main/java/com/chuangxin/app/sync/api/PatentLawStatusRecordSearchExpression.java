package com.chuangxin.app.sync.api;

import com.chuangxin.app.function.*;
import com.chuangxin.bean.api.BasePageExpressPO;
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

/**
 * 和另一个法律接口比较多了，少了一些专利的信息，上个接口像是汇总了这个接口的数据，就是把所有的授权拼在一起了
 * "pds": "2022/01/04 00:00:00", 申请公布日
 * "pdq": "2022/06/07 00:00:00", 授权公布日
 * "apo": "杭州雯汐科技有限公司", 申请人
 * 然后 "ilsad": "2023/02/03 00:00:00",不再是拼接的字符串了
 */
public class PatentLawStatusRecordSearchExpression {
    public static void main(String[] args) throws Exception {
        BaseExpressionContext context = new BaseExpressionContext("FLINK-SYNC:PATENT_LAW_RECORD_STATUS_SEARCH_EXPRESSION");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.out.println("当前法律公告日:" + context.maxDt);
        System.out.printf("当前%s:%s%n", context.incCn, context.maxDt);
        BasePageExpressPO basePageExpressPO = new BasePageExpressPO(context.incCol);

        HttpSourceFunction sourceFunction = context.getHttpPageSourceFunction("/api/patent/lawStatusRecordSearch/expression", basePageExpressPO);
        DataStreamSource<Tuple2<Map<String, String>, String>> streamSource = env.addSource(sourceFunction);
        //为了使用状态增加虚拟keyby
        KeyedStream<String, Object> keyedStream = streamSource.map(x -> x.f1).keyBy((KeySelector<String, Object>) value -> "dummyKey");
        SingleOutputStreamOperator<String> recordsStream = keyedStream.flatMap(new BaseExpressionRichFlatMapFunction(context));
        DataStream<Document> documents = recordsStream.map((MapFunction<String, Document>) Document::parse);
        //写入mongoDB
        documents.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, context.taskName));
        env.execute(context.taskName);
    }
} 


