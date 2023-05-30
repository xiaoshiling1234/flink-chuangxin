package com.chuangxin.util;

import com.alibaba.fastjson.JSONObject;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

public class TokenUtil {
    public static Map<String, String> getToken(){
        try (Jedis jedis = RedisUtil.getJedis()) {
            String redisKey = "authParams";
            return jedis.hgetAll(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        StringRedisSerializer serializer = new StringRedisSerializer();
        Objects.requireNonNull(getToken()).forEach((k, v)->{
            System.out.println(serializer.deserialize(k.getBytes()));
            System.out.println(serializer.deserialize(v.getBytes()));
        });
    }
}
