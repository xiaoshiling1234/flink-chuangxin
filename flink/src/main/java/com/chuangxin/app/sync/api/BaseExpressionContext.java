package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.app.function.MongoDBSink;
import com.chuangxin.bean.ImageDownBean;
import com.chuangxin.bean.api.BasePageExpressPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.BsonUtil;
import com.chuangxin.util.HttpClientUtils;
import com.chuangxin.util.MysqlUtil;
import com.chuangxin.util.ObjectUtil;
import com.squareup.okhttp.Response;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Data
public class BaseExpressionContext implements Serializable {
    String taskName;
    String maxDt;
    String incCn;
    String incCol;

    public BaseExpressionContext(String taskName) {
        this.taskName = taskName;
        Map<String, Object> incInfo = getIncInfo();
        this.maxDt = incInfo.get("max_dt").toString();
        this.incCn = incInfo.get("inc_cn").toString();
        this.incCol = incInfo.get("inc_col").toString();
    }

    private Map<String, Object> getIncInfo() {
        String sql = String.format("SELECT * from task where  task_name='%s'", taskName);
        return MysqlUtil.query(sql).get(0);
    }

    private int getPageCountAndUpdateExpression(String url, BasePageExpressPO basePageExpressPO) throws IllegalAccessException, IOException {
        //这个接口需要增量拉取，所以增加时间筛选
        //使用大于等于预防一次拉不玩的情况
        String express = String.format("%s AND (%s>=%s)", basePageExpressPO.getExpress(), incCn, maxDt);
        basePageExpressPO.setExpress(express);
        System.out.println(url + "\n" + ObjectUtil.objectToMap(basePageExpressPO));
        Response response = HttpClientUtils.doGet(url, ObjectUtil.objectToMap(basePageExpressPO));
        String responseData = response.body().string();
        JSONObject jsonObject = JSON.parseObject(responseData);
        if (Objects.equals(jsonObject.getString("total"), "")) {
            return 0;
        }
        int pageRow = Integer.parseInt(jsonObject.getString("page_row"));
        int total = Integer.parseInt(jsonObject.getString("total"));
        System.out.println("数据总条数:" + total);
        return total >= 10000 ? 200 : total / pageRow + 1;
    }

    public HttpSourceFunction getHttpPageSourceFunction(String url, BasePageExpressPO basePageExpressPO) {
        return new HttpSourceFunction(GlobalConfig.API_BASH_URL + url) {
            @Override
            public List<Map<String, String>> getRequestParametersList() throws IllegalAccessException, IOException {
                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                int pageCount = getPageCountAndUpdateExpression(url, basePageExpressPO);
                System.out.println("分页数为:" + pageCount);
                for (int i = 1; i <= pageCount; i++) {
                    basePageExpressPO.setPage(String.valueOf(i));
                    requestJsonList.add(ObjectUtil.objectToMap(basePageExpressPO));
                }
                return requestJsonList;
            }
        };
    }

    public void processImage(OutputTag<ImageDownBean> outputTag, SingleOutputStreamOperator<Document> documents) {
        OutputTag<ImageDownBean> failTag = new OutputTag<ImageDownBean>("FailTask") {
        };
        // 图片下载任务写入Kafka
        SingleOutputStreamOperator<Document> downImageStream = documents.getSideOutput(outputTag).process(new ProcessFunction<ImageDownBean, Document>() {
            private ExecutorService executorService;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化线程池
                executorService = Executors.newFixedThreadPool(10);
            }

            @Override
            public void close() throws Exception {
                super.close();
                // 关闭线程池
                if (executorService != null) {
                    executorService.shutdown();
                }
            }

            @Override
            public void processElement(ImageDownBean imageDownBean, ProcessFunction<ImageDownBean, Document>.Context context, Collector<Document> collector) {

                String imageUrl = imageDownBean.getImageUrl();
                Document document = BsonUtil.toDocument(imageDownBean);
                if (imageUrl == null || !imageUrl.startsWith("http")) {
                    imageDownBean.setErrorInfo("非法的图片地址");
                    imageDownBean.setDownStatus(0);
                    if (imageDownBean.getDownStatus() == 0) {
                        context.output(failTag, imageDownBean);
                    }
                    return;
                }

                // 使用线程池提交下载任务
                executorService.submit(() -> {
                    try {
                        byte[] image = downloadImage(imageUrl);
                        document.append("imageByte", image);
                        collector.collect(document);
                    } catch (Exception e) {
                        e.printStackTrace();
                        imageDownBean.setErrorInfo(e.getMessage());
                        imageDownBean.setDownStatus(0);
                        context.output(failTag, imageDownBean);
                    }
                });
            }
        });

        // 成功下载的数据存储到mongdb
        downImageStream.addSink(new MongoDBSink(GlobalConfig.MONGODB_SYNC_DBNAME, GlobalConfig.MONGODB_IMAGE_COLLECTION)).name("MongoDB Sink");

        //存储所有的异常信息
        downImageStream.getSideOutput(failTag).addSink(
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
                                .withBatchIntervalMs(5000)
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


    private byte[] downloadImage(String imageUrl) throws IOException {
        URL url = new URL(imageUrl);
        Path tempFile = Files.createTempFile("image", ".jpg");
        try (InputStream inputStream = url.openStream()) {
            Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }
        byte[] imageBytes = Files.readAllBytes(tempFile);
        Files.delete(tempFile);
        return imageBytes;
    }
}
