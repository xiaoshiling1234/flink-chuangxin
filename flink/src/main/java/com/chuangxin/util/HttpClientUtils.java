package com.chuangxin.util;

import com.chuangxin.bean.Ignore;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

import java.io.IOException;
import java.util.HashMap;
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
}