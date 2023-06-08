
package com.chuangxin.schedule;


import com.alibaba.fastjson.JSON;
import com.chuangxin.util.HttpClientUtils;
import com.squareup.okhttp.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class RefreshToken {
    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    public Map<String, Object> getToken(String code) throws IOException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("client_id","6caa041c0a0a01092042d2f59e8c7118");
        parameters.put("code",code);
        parameters.put("client_secret","6caa041d0a0a01092042d2f593a602ba");
        parameters.put("grant_type","authorization_code");
        parameters.put("redirect_uri","http://www.fastsay.cn:9999/auth/callback");

        Response response = HttpClientUtils.doPost("http://114.251.8.193/oauth/token",parameters);
        Map<String, Object> innerMap = JSON.parseObject(response.body().string()).getInnerMap();
        redisTemplate.delete("token");
        redisTemplate.opsForHash().putAll("token",innerMap);
        return innerMap;
    }

    public Map<Object, Object> refreshToken() throws IOException {
        Map<Object, Object> token = redisTemplate.opsForHash().entries("token");
        String refreshToken = (String) token.get("refresh_token");
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("client_id","6caa041c0a0a01092042d2f59e8c7118");
        parameters.put("refresh_token",refreshToken);
        parameters.put("client_secret","6caa041d0a0a01092042d2f593a602ba");
        parameters.put("grant_type","refresh_token");

        Response response = HttpClientUtils.doPost("http://114.251.8.193/oauth/token",parameters);
        Map<String, Object> innerMap = JSON.parseObject(response.body().string()).getInnerMap();
        redisTemplate.delete("token");
        redisTemplate.opsForHash().putAll("token",innerMap);
        return token;
    }
}




