package com.chuangxin.app.sync.img;

import com.alibaba.fastjson.JSON;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.ImageDownBean;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.BsonUtil;
import com.chuangxin.util.MyKafkaUtil;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.SerializableSupplier;
import org.bson.Document;

import javax.sql.XADataSource;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ImageDownRealTask {
    public static void main(String[] args) throws Exception {
        // 设置Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoints/imagedown_realtask/ck"));
//        env.enableCheckpointing(60000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(600000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        KafkaSource<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(GlobalConfig.KAFKA_IMAGE_SOURCE_TOPIC, GlobalConfig.KAFKA_IMAGE_SOURCE_GROUP_ID);
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<ImageDownBean> imageDownBeanStream = kafkaSource.map((MapFunction<String, ImageDownBean>) s -> JSON.parseObject(s, ImageDownBean.class));
        OutputTag<ImageDownBean> outputTag = new OutputTag<ImageDownBean>("FailTask") {
        };

        SingleOutputStreamOperator<Document> downImageStream = imageDownBeanStream.process(new ProcessFunction<ImageDownBean, Document>() {
            @Override
            public void processElement(ImageDownBean imageDownBean, ProcessFunction<ImageDownBean, Document>.Context context, Collector<Document> collector) {
                String imageUrl = imageDownBean.getImageUrl();
                Document document = BsonUtil.toDocument(imageDownBean);
                byte[] image;
                if (imageUrl == null || !imageUrl.startsWith("http")) {
                    imageDownBean.setErrorInfo("非法的图片地址");
                    imageDownBean.setDownStatus(0);
                } else {
                    try {
                        image = downloadImage(imageUrl);
                        document.append("imageByte", image);
                        imageDownBean.setDownStatus(1);
                        collector.collect(document);
                    } catch (Exception e) {
                        e.printStackTrace();
                        imageDownBean.setErrorInfo(e.getMessage());
                        imageDownBean.setDownStatus(0);
                        context.output(outputTag, imageDownBean);
                    }
                }
                context.output(outputTag, imageDownBean);
            }
        });

        downImageStream.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, GlobalConfig.MONGODB_IMAGE_COLLECTION)).name("MongoDB Sink");

        downImageStream.getSideOutput(outputTag).addSink(
                JdbcSink.sink(
                        "insert into image_await_task (task_name, key_field, key_value, image_field_name, image_url , down_status, error_info) values (? ,? , ? ,? ,? ,? ,?)",
                        (statement, imageDownBean) -> {
                            statement.setString(1, imageDownBean.getTaskName());
                            statement.setString(2, imageDownBean.getKeyField());
                            statement.setString(3, imageDownBean.getKeyValue());
                            statement.setString(4, imageDownBean.getImageFieldName());
                            statement.setString(5, imageDownBean.getImageUrl());
                            statement.setInt(6, imageDownBean.getDownStatus());
                            statement.setString(7, imageDownBean.getErrorInfo());
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
        byte[] imageBytes = Files.readAllBytes(tempFile);
        Files.delete(tempFile);
        return imageBytes;
    }
}
