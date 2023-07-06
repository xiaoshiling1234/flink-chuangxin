package com.chuangxin.app.sync.img;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bson.Document;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class ImageDownRealTask {
    public static void main(String[] args) throws Exception {
        // 设置Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaConsumer(GlobalConfig.KAFKA_IMAGE_SOURCE_TOPIC, GlobalConfig.KAFKA_IMAGE_SOURCE_GROUP_ID));

        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.map((MapFunction<String, JSONObject>) JSON::parseObject);

        OutputTag<String> outputTag = new OutputTag<String>("FailTask") {
        };
        SingleOutputStreamOperator<Document> downImageStream = jsonStream.process(new ProcessFunction<JSONObject, Document>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, Document>.Context ctx, Collector<Document> out) {
                String imageUrl = jsonObject.getString("imageUrl");
                Document document = Document.parse(jsonObject.toJSONString());
                byte[] image;
                if (imageUrl==null||!imageUrl.startsWith("http")){
                    ctx.output(outputTag, JSON.toJSONString(document));
                }else {
                    try {
                        image = downloadImage(jsonObject.getString("imageUrl"));
                        document.append("imageByte", image);
                        out.collect(document);
                    }catch (Exception e){
                        e.printStackTrace();
                        ctx.output(outputTag, JSON.toJSONString(document));
                    }
                }
            }
        });

        downImageStream.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, GlobalConfig.MONGODB_IMAGE_COLLECTION)).name("MongoDB Sink");
        jsonStream.print("Success-------");
        downImageStream.getSideOutput(outputTag).print("Fail>>>>>>>>>>>");
        downImageStream.getSideOutput(outputTag).addSink(MyKafkaUtil.getKafkaProducer(GlobalConfig.KAFKA_IMAGE_FAIl_SOURCE_TOPIC));
        // 执行Flink作业
        env.execute("FLINK-SYNC:ImageDownRealTask");
    }

    private static byte[] downloadImage(String imageUrl) throws IOException {
        URL url = new URL(imageUrl);
        Path tempFile = Files.createTempFile("image", ".jpg");
        Files.copy(url.openStream(), tempFile, StandardCopyOption.REPLACE_EXISTING);
        return Files.readAllBytes(tempFile);
    }
}
