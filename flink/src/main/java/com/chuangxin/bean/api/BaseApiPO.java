package com.chuangxin.bean.api;


import com.chuangxin.common.GlobalConfig;

public class BaseApiPO {
    String client_id= GlobalConfig.API_CLIENT_ID;
    String access_token=GlobalConfig.API_ACCESS_TOKEN;
    String scope=GlobalConfig.API_SCOPE;

    public String getClient_id() {
        return client_id;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public String getAccess_token() {
        return access_token;
    }

    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }
}
