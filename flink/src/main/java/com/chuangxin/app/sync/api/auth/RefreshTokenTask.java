package com.chuangxin.app.sync.api.auth;

import com.chuangxin.util.TokenUtil;

import java.io.IOException;

public class RefreshTokenTask {
    public static void main(String[] args) throws IOException {
        TokenUtil.refreshToken();
    }
}
