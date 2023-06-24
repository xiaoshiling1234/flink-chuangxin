package com.chuangxin.app.sync.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chuangxin.app.function.HttpSourceFunction;
import com.chuangxin.bean.api.BasePageExpressPO;
import com.chuangxin.common.GlobalConfig;
import com.chuangxin.util.HttpClientUtils;
import com.chuangxin.util.MysqlUtil;
import com.chuangxin.util.ObjectUtil;
import com.squareup.okhttp.Response;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
public class BaseExpressionContext implements Serializable {
    String taskName;
    String maxDt;
    String incCn;
    String incCol;

    public BaseExpressionContext(String taskName) {
        this.taskName = taskName;
        Map<String, Object> incInfo = getIncInfo();
        this.maxDt = incInfo.get("max_dt").toString();
        this.incCn = incInfo.get("inc_cn").toString();
        this.incCol = incInfo.get("inc_col").toString();
    }

    private Map<String, Object> getIncInfo() {
        String sql = String.format("SELECT * from task where  task_name='%s'", taskName);
        return MysqlUtil.query(sql).get(0);
    }

    private int getPageCountAndUpdateExpression(String url, BasePageExpressPO basePageExpressPO) throws IllegalAccessException, IOException {
        //这个接口需要增量拉取，所以增加时间筛选
        //使用大于等于预防一次拉不玩的情况
        String express = String.format("%s AND (%s>=%s)", basePageExpressPO.getExpress(), incCn, maxDt);
        basePageExpressPO.setExpress(express);
        System.out.println(url+"\n"+ObjectUtil.objectToMap(basePageExpressPO));
        Response response = HttpClientUtils.doGet(url, ObjectUtil.objectToMap(basePageExpressPO));
        String responseData = response.body().string();
        JSONObject jsonObject = JSON.parseObject(responseData);
        if (Objects.equals(jsonObject.getString("total"), "")) {
            return 0;
        }
        int pageRow = Integer.parseInt(jsonObject.getString("page_row"));
        int total = Integer.parseInt(jsonObject.getString("total"));
        System.out.println("数据总条数:" + total);
        return total / pageRow + 1;
    }

    public HttpSourceFunction getHttpPageSourceFunction(String url, BasePageExpressPO basePageExpressPO) {
        return new HttpSourceFunction(GlobalConfig.API_BASH_URL + url) {
            @Override
            public List<Map<String, String>> getRequestParametersList() throws IllegalAccessException, IOException {
                ArrayList<Map<String, String>> requestJsonList = new ArrayList<>();
                int pageCount = getPageCountAndUpdateExpression(url, basePageExpressPO);
                System.out.println("分页数为:" + pageCount);
                for (int i = 1; i <= pageCount; i++) {
                    basePageExpressPO.setPage(String.valueOf(i));
                    requestJsonList.add(ObjectUtil.objectToMap(basePageExpressPO));
                }
                return requestJsonList;
            }
        };
    }
}
