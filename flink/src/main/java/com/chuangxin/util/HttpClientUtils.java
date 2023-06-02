package com.chuangxin.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.bean.Ignore;
import com.chuangxin.bean.api.PatentSearchExpressionPO;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpClientUtils {
    public static Response doGet(String url,Map<String, String> params) throws IOException {
        OkHttpClient client = new OkHttpClient();
        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
        }
        String fullURl = urlBuilder.build().toString();
        Request request = new Request.Builder()
                .url(fullURl)
                .method("GET", null)
                .addHeader("User-Agent", "Apifox/1.0.0 (https://apifox.com)")
                .addHeader("Accept", "*/*")
                .addHeader("Host", "114.251.8.193")
                .addHeader("Connection", "keep-alive")
                .build();
        return client.newCall(request).execute();
    }

    public static Map<String, String> objectToMap(Object obj) throws IllegalAccessException {
        Map<String, String> map = new HashMap<>();
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                Object value = field.get(obj);
                if (value != null && !field.isAnnotationPresent(Ignore.class)) {
                    map.put(field.getName(), value.toString());
                }
            }
            clazz = clazz.getSuperclass();
        }
        return map;
    }


    public static void main(String[] args) throws IllegalAccessException, IOException {
        PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
        String url="http://114.251.8.193/api/patent/search/expression";
        Response response = HttpClientUtils.doGet(url, HttpClientUtils.objectToMap(patentSearchExpressionPO));
        String responseData = response.body().string();
        JSONObject jsonObject = JSON.parseObject(responseData);
        System.out.println(jsonObject);
        System.out.println(response);
//        PatentSearchExpressionPO patentSearchExpressionPO = new PatentSearchExpressionPO();
//        Map<String, String> stringStringMap = HttpClientUtils.objectToMap(patentSearchExpressionPO);
//        System.out.println(stringStringMap);
    }
}