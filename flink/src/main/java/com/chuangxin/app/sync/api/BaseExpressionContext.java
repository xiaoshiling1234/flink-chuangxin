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
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BaseExpressionContext implements Serializable {
    String taskName;
    String maxDt;
    String incCol;

    public BaseExpressionContext(String taskName) {
        this.taskName = taskName;
        Tuple2<String, String> incInfo = getIncInfo();
        this.maxDt = incInfo.f0;
        this.incCol = incInfo.f1;
    }

    private Tuple2<String, String> getIncInfo() {
        String sql = String.format("SELECT * from task where  task_name='%s'", taskName);
        List<Map<String, Object>> query = MysqlUtil.query(sql);
        return new Tuple2<>(query.get(0).get("max_dt").toString(), query.get(0).get("inc_col").toString());
    }

    private int getPageCountAndUpdateExpression(String url, BasePageExpressPO basePageExpressPO) throws IllegalAccessException, IOException {
        //这个接口需要增量拉取，所以增加时间筛选
        //使用大于等于预防一次拉不玩的情况
        String express = String.format("%s AND (%s>=%s)", basePageExpressPO.getExpress(), incCol, maxDt);
        basePageExpressPO.setExpress(express);
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
