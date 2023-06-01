package com.chuangxin.bean.api;

public class PatentSearchExpressionPO extends BaseApiPO{
    String express="((名称+摘要和说明+权利要求书+说明书全文=无人机) OR (关键词=无人机) OR (技术领域=无人机) OR (背景技术=无人机) OR (发明内容=无人机) OR (具体实施方式=无人机) OR (附图说明=无人机))";
    String page="1";
    String sort_column="+PD";
    String exactSearch="1";
    String page_row="50";

    public String getExpress() {
        return express;
    }

    public void setExpress(String express) {
        this.express = express;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getSort_column() {
        return sort_column;
    }

    public void setSort_column(String sort_column) {
        this.sort_column = sort_column;
    }

    public String getExactSearch() {
        return exactSearch;
    }

    public void setExactSearch(String exactSearch) {
        this.exactSearch = exactSearch;
    }

    public String getPage_row() {
        return page_row;
    }

    public void setPage_row(String page_row) {
        this.page_row = page_row;
    }
}
