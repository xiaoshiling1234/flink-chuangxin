package com.chuangxin.bean.api;

public class BasePageExpressPO extends BaseApiPO{
    String express="(\n" +
            " (名称,摘要和说明,权利要求书,说明书全文 +=  \n" +
            " ( '无人驾驶飞机' or  '无人机' or 'unmanned aerial vehicle' or 'drone' or  '无人飞机'  OR 'UAV' or  'unmanned drone'  or '不载人飞机' or '无人驾驶飞行器'or '无人飞行器'or '无人直升机' or '无人直升飞机'  or '无人飞行系统'\n" +
            ") \n" +
            " )\n" +
            "or \n" +
            "(名称,摘要和说明,权利要求书,说明书全文 += ('无人' or '不载人') and ('飞机' or '飞行系统' or '飞行器' or '直升机' or '直升飞机' or '航空器'or '航空装备') ) \n" +
            "or   \n" +
            "(分类号 =  ( B64U% ) )\n" +
            ") ";
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
