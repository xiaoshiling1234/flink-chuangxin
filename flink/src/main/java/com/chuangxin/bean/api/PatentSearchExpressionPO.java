package com.chuangxin.bean.api;

import java.io.Serializable;

public class PatentSearchExpressionPO extends BasePageExpressPO{
    String exactSearch="1";

    public String getExactSearch() {
        return exactSearch;
    }

    public void setExactSearch(String exactSearch) {
        this.exactSearch = exactSearch;
    }
}
