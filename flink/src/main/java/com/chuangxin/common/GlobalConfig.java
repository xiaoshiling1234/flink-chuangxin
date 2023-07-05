package com.chuangxin.common;

public class GlobalConfig {
    /**
     * MYSQL配置
     */
    public static final String MYSQL_URL="jdbc:mysql://bigdata01:3306/chuangxin?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    public static final String MYSQL_USER="root";
    public static final String MYSQL_PASSWORD="root%123";
    /**
     * MONGODB配置
     */
    public static final String MONGODB_URI="mongodb://sync:sync3332141241211@bigdata01:27017/sync";
    public static final String MONGODB_SYNC_DBNAME="sync";
    public static final String MONGODB_IMAGE_COLLECTION="IMAGE";
    /**
     * DI开放平台配置
     */
    public static final String API_BASH_URL ="http://114.251.8.193";
    public static final String API_CLIENT_ID="6caa041c0a0a01092042d2f59e8c7118";
    public static final String API_SCOPE="read_cn";
    public static final String API_SECRET="6caa041d0a0a01092042d2f593a602ba";

    /**
     * Kafka配置
     */
    public static final String IMAGE_SOURCE_TOPIC="6caa041d0a0a01092042d2f593a602ba";
    public static final String IMAGE_SOURCE_GROUP_ID="IMAGE_SOURCE";
}
