package com.chuangxin.app.sync.img;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.MyKafkaUtil;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

public class ImageDownRealTask {
    public static void main(String[] args) throws Exception {
        // 设置Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaConsumer(GlobalConfig.IMAGE_SOURCE_TOPIC, GlobalConfig.IMAGE_SOURCE_GROUP_ID));

        // 解析Kafka数据，提取图片地址
        DataStream<Tuple2<String, String>> imageStream = kafkaStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String imageUrl = jsonObject.getString("image_url");
                String localPath = generateLocalPath(imageUrl);
                return new Tuple2<>(imageUrl,localPath);
            }
        });

        // 下载图片
        SingleOutputStreamOperator<Document> downloadedImages = imageStream.map(new MapFunction<Tuple2<String, String>, Document>() {
            @Override
            public Document map(Tuple2<String, String> value) throws Exception {
                String imageUrl = value.f0;
                String localPath = value.f1;

                // 在这里根据图片地址下载图片
                downloadImage(imageUrl, localPath);
                return new Document().append("imageUrl", imageUrl).append("localPath",localPath);
            }
        });

        downloadedImages.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, GlobalConfig.MONGODB_IMAGE_COLLECTION)).name("MongoDB Sink");
        // 执行Flink作业
        env.execute("KafkaToFlinkToMongoDB");
    }

    private static String generateLocalPath(String imageUrl) {
        return imageUrl;
    }

    private static void downloadImage(String imageUrl, String localPath) throws IOException {
        URL url = new URL(imageUrl);
        try (BufferedInputStream in = new BufferedInputStream(url.openStream());
             OutputStream out = new FileOutputStream(localPath)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, 1024)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }
}
