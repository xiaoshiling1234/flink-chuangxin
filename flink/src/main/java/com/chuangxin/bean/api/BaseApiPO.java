package com.chuangxin.bean.api;


import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.TokenUtil;

public class BaseApiPO {
    String client_id= GlobalConfig.API_CLIENT_ID;
    String access_token= TokenUtil.getCurrentToken().getAccessToken();
    String scope=GlobalConfig.API_SCOPE;
}
