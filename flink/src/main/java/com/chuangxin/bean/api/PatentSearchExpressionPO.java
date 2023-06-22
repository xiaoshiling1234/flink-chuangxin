package com.chuangxin.bean.api;

import java.io.Serializable;

public class PatentSearchExpressionPO extends BasePageExpressPO implements Serializable {
    String sort_column="+PD";
    String exactSearch="1";

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
}
