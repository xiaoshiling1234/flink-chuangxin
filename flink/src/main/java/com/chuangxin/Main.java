
package com.chuangxin;

import com.chuangxin.bean.Ignore;
import com.squareup.okhttp.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.chuangxin.util.DateTimeUtil.convertDateFormat;

public class Main {
    public static void main(String[] args) throws IOException {
        String s1=convertDateFormat("2023/04/06 00:00:00");
        System.out.println(s1);
        String s2="202305";

        int i = s1.compareTo(s2);
        System.out.println(i);
    }
    

    
}


