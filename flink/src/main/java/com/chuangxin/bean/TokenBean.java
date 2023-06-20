package com.chuangxin.bean;

import com.chuangxin.common.GlobalConfig;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TokenBean {
    String accessToken;
    String refreshToken;
    String scope;
    String tokenType;
    Integer expiresIn;
}
