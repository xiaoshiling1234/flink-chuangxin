package com.chuangxin;


import com.chuangxin.schedule.RefreshToken;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest()
public class CommonTest {
    @Autowired
    RefreshToken refreshToken;

    @Test
    void getToken() throws IOException {
        Map<String, Object> token = refreshToken.getToken("RWlrJa");
        System.out.println(token);
    }

    @Test
    void refreshToken() throws IOException {
        Map<Object, Object> objectObjectMap = refreshToken.refreshToken();
        System.out.println(objectObjectMap);
    }
}
