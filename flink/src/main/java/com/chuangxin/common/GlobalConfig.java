package com.chuangxin.common;

public class GlobalConfig {
    /**
     * REDIS配置
     */
    public static final String REDIS_HOST="www.fastsay.cn";
    public static final Integer REDIS_PORT=6379;
    public static final String REDIS_PASSWORD="123456";
    public static final Integer REDIS_DATABASE=0;

    /**
     * MYSQL配置
     */
    public static final String MYSQL_URL="jdbc:mysql://www.fastsay.cn:3306/chuangxin?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    public static final String MYSQL_USER="chuangxin";
    public static final String MYSQL_PASSWORD="root%123";
    /**
     * MONGODB配置
     */
    public static final String MONGODB_URI="mongodb://sync:sync333214124121@fastsay.cn:27017/sync";
    public static final String MONGODB_SYNC_DBNAME="sync";
    /**
     * DI开放平台配置
     */
    public static final String API_BASH_URL ="http://114.251.8.193";
    public static final String API_CLIENT_ID="6caa041c0a0a01092042d2f59e8c7118";
    public static final String API_SCOPE="read_cn";
    public static final String API_SECRET="6caa041d0a0a01092042d2f593a602ba";
    //默认公布日
    public static final String API_DEFAULT_PD="202305";
    //默认法律公告日
    public static final String API_DEFAULT_ILSAD="202305";

}
