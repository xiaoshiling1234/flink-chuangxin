
package com.chuangxin;

import com.chuangxin.bean.Ignore;
import com.squareup.okhttp.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        OkHttpClient client = new OkHttpClient();
        Map<String, String> params = new HashMap<>();
        params.put("client_id", "6caa041c0a0a01092042d2f59e8c7118");
        params.put("access_token", "03635666-f281-4613-992e-b815b4689d03");
        params.put("scope", "read_cn");
        params.put("express", "(（名称 摘要和说明 权利要求书 说明书全文=无人机） OR （关键词=无人机） OR （技术领域=无人机） OR （背景技术=无人机） OR （发明内容=无人机） OR （具体实施方式=无人机） OR （附图说明=无人机））");
        params.put("page", "1");
        params.put("sort_column", "+PD");
        params.put("exactSearch", "1");
        params.put("page_row", "10");
        HttpUrl.Builder urlBuilder = HttpUrl.parse("http://114.251.8.193/api/patent/search/expression").newBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
        }
        String url = urlBuilder.build().toString();
        Request request = new Request.Builder()
                .url(url)
                .method("GET", null)
                .addHeader("User-Agent", "Apifox/1.0.0 (https://apifox.com)")
                .addHeader("Accept", "*/*")
                .addHeader("Host", "114.251.8.193")
                .addHeader("Connection", "keep-alive")
                .build();
        Response response = client.newCall(request).execute();
        System.out.println(response);
    }
    

    
}


