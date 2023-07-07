package com.chuangxin.app.sync.img;

import com.alibaba.fastjson.JSON;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.ImageDownBean;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.BsonUtil;
import com.chuangxin.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
        env.setStateBackend(new FsStateBackend("file:///opt/modules/flink/data/checkpoint/imagedownrealtask/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaConsumer(GlobalConfig.KAFKA_IMAGE_SOURCE_TOPIC, GlobalConfig.KAFKA_IMAGE_SOURCE_GROUP_ID));
        SingleOutputStreamOperator<ImageDownBean> imageDownBeanStream = kafkaStream.map((MapFunction<String, ImageDownBean>) s -> JSON.parseObject(s, ImageDownBean.class));
        OutputTag<ImageDownBean> outputTag = new OutputTag<ImageDownBean>("FailTask") {
        };

        SingleOutputStreamOperator<Document> downImageStream = imageDownBeanStream.process(new ProcessFunction<ImageDownBean, Document>() {
            @Override
            public void processElement(ImageDownBean imageDownBean, ProcessFunction<ImageDownBean, Document>.Context context, Collector<Document> collector) {
                String imageUrl = imageDownBean.getImageUrl();
                Document document = BsonUtil.toDocument(imageDownBean);
                byte[] image;
                if (imageUrl == null || !imageUrl.startsWith("http")) {
                    context.output(outputTag, imageDownBean);
                } else {
                    try {
                        image = downloadImage(imageUrl);
                        document.append("imageByte", image);
                        collector.collect(document);
                    } catch (Exception e) {
                        e.printStackTrace();
                        context.output(outputTag, imageDownBean);
                    }
                }
            }
        });

        downImageStream.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, GlobalConfig.MONGODB_IMAGE_COLLECTION)).name("MongoDB Sink");
        downImageStream.getSideOutput(outputTag).addSink(
                JdbcSink.sink(
                        "insert into image_await_task (task_name, key_field, key_value, image_field_name, image_url) values (?, ? ,? ,? ,?)",
                        (statement, imageDownBean) -> {
                            statement.setString(1,imageDownBean.getTaskName());
                            statement.setString(2,imageDownBean.getKeyField());
                            statement.setString(3,imageDownBean.getKeyValue());
                            statement.setString(4,imageDownBean.getImageFieldName());
                            statement.setString(5,imageDownBean.getImageUrl());
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
