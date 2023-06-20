package com.chuangxin.util;

import com.alibaba.fastjson.JSON;
import com.chuangxin.bean.TokenBean;
import com.chuangxin.common.GlobalConfig;
import com.squareup.okhttp.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TokenUtil {
    public static TokenBean getCurrentToken() {
        List<Map<String, Object>> query = MysqlUtil.query("select access_token, token_type, refresh_token, expires_in, scope from token");
        Map<String, Object> tokenInfo = query.get(0);
        return new TokenBean(String.valueOf(tokenInfo.get("access_token")),
                String.valueOf(tokenInfo.get("refresh_token")),
                String.valueOf(tokenInfo.get("scope")),
                String.valueOf(tokenInfo.get("token_type")),
                (Integer) tokenInfo.get("expires_in"));
    }

    public static TokenBean refreshToken() throws IOException {
        TokenBean currentToken = getCurrentToken();
        String refreshToken = currentToken.getRefreshToken();

        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("client_id", GlobalConfig.API_CLIENT_ID);
        parameters.put("refresh_token", refreshToken);
        parameters.put("client_secret", GlobalConfig.API_SECRET);
        parameters.put("grant_type", "refresh_token");
        Response response = HttpClientUtils.doPost(GlobalConfig.API_BASH_URL + "/oauth/token", parameters);
        Map<String, Object> tokenInfo = JSON.parseObject(response.body().string()).getInnerMap();
        MysqlUtil.update("token", tokenInfo, null);
        return new TokenBean(String.valueOf(tokenInfo.get("access_token")),
                String.valueOf(tokenInfo.get("refresh_token")),
                String.valueOf(tokenInfo.get("scope")),
                String.valueOf(tokenInfo.get("token_type")),
                (Integer) tokenInfo.get("expires_in"));
    }

}
