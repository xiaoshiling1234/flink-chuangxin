
package com.chuangxin.app.function;

import com.chuangxin.common.GlobalConfig;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.Collections;

public class MongoDBSink extends RichSinkFunction<Document> {
    private final String mongodbDbName;
    private final String mongodbCollectionName;

    public MongoDBSink(String mongodbDbName, String mongodbCollectionName) {
        this.mongodbDbName = mongodbDbName;
        this.mongodbCollectionName = mongodbCollectionName;
    }

    private transient MongoClient mongoClient;
    private transient MongoDatabase database;
    private transient MongoCollection<Document> collection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建MongoDB客户端连接
        MongoClientURI mongoClientURI = new MongoClientURI(GlobalConfig.MONGODB_URI);
        this.mongoClient = new MongoClient(mongoClientURI);
        // 获取指定名称的数据库对象
        this.database = mongoClient.getDatabase(mongodbDbName);
        // 获取指定名称的集合对象
        this.collection = database.getCollection(mongodbCollectionName);
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 关闭MongoDB客户端连接
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Override
    public void invoke(Document document, Context context) {
        // 插入文档数据到MongoDB集合中
        collection.insertOne(document);
    }
}


