package com.chuangxin.bean.api;

public class BasePageExpressPO extends BaseApiPO{
    String express="(名称=无人机)";
    String page="1";
    String page_row="50";
    String sort_column;

    public BasePageExpressPO() {
    }

    public BasePageExpressPO(String sort_column) {
        this.sort_column="+"+sort_column.toUpperCase();
    }

    public String getPage_row() {
        return page_row;
    }

    public String getExpress() {
        return express;
    }

    public String getPage() {
        return page;
    }

    public void setExpress(String express) {
        this.express = express;
    }
    public void setPage(String page) {
        this.page = page;
    }

    public void setPage_row(String page_row) {
        this.page_row = page_row;
    }

    public void setSort_column(String sort_column) {
        this.sort_column = sort_column;
    }

    public String getSort_column() {
        return sort_column;
    }
}
