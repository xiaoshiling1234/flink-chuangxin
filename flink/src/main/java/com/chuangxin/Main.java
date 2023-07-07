
package com.chuangxin;

import com.alibaba.fastjson.JSON;
import com.chuangxin.bean.Ignore;
import com.chuangxin.bean.ImageDownBean;
import com.squareup.okhttp.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.chuangxin.util.DateTimeUtil.convertDateFormat;

public class Main {
    public static void main(String[] args) throws IOException {
        String s="{\"imageFieldName\":\"IMGO\",\"imageUrl\":\"http://pt.cnipr.com/res/b670c6b76d8f54cea1f1dc87c0167d42/78FF4A/D82913/719687439/1688757942/1200/pi13/PAT/176/73/CN2021108157134B_20210518/HDA0001506276150000011.GIF\",\"keyField\":\"pid\",\"keyValue\":\"PIDCNB02021051800000000108157155I25G7P6013BC0\",\"taskName\":\"FLINK-SYNC:PATENT_PLEDGE_SEARCH_EXPRESSION\"}\n";


        System.out.println(JSON.parseObject(s, ImageDownBean.class));
    }
    

    
}


