package com.chuangxin.bean.api;


import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.TokenUtil;

import java.io.Serializable;

public class BaseApiPO implements Serializable {
    String client_id= GlobalConfig.API_CLIENT_ID;
    String access_token= TokenUtil.getCurrentToken().getAccessToken();
    String scope=GlobalConfig.API_SCOPE;
}
