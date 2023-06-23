package com.chuangxin.bean.api;

import java.io.Serializable;

public class PatentTransferSearchPO extends BasePageExpressPO implements Serializable {
    String sort_column="+PD";

    public String getSort_column() {
        return sort_column;
    }

    public void setSort_column(String sort_column) {
        this.sort_column = sort_column;
    }
}
