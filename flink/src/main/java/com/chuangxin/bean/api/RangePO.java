package com.chuangxin.bean.api;

import com.chuangxin.util.DateTimeUtil;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public class RangePO implements Serializable {
    String beginDate;
    String endDate;
    Integer startPage;
    Integer endPage;

    public RangePO(ParameterTool parameterTool) {
        this.beginDate = parameterTool.get("beginDate", DateTimeUtil.getYesterdayYMD());
        this.endDate = parameterTool.get("endDate", DateTimeUtil.getYesterdayYMD());
        this.startPage = parameterTool.getInt("startPage", 1);
        this.endPage = parameterTool.getInt("endPage", Integer.MAX_VALUE);
    }

    public String getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public Integer getStartPage() {
        return startPage;
    }

    public void setStartPage(Integer startPage) {
        this.startPage = startPage;
    }

    public Integer getEndPage() {
        return endPage;
    }

    public void setEndPage(Integer endPage) {
        this.endPage = endPage;
    }
}
