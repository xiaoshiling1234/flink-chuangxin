package com.chuangxin.controller;

import com.chuangxin.entity.enums.ResEnum;
import com.chuangxin.util.HttpRes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

//@RestController
@Controller
@CrossOrigin
@RequestMapping("/auth")
public class AuthController {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @GetMapping("/callback")
    public String callbackHtml(Model model) {
        model.addAttribute("hello","创新大数据");
        return "success";
    }

    @GetMapping("/saveToRedis")
    @ResponseBody
    public HttpRes saveToRedis(HttpServletRequest request) {
        try {
            Map<String, String[]> params = request.getParameterMap();
            //将params写入redis
            redisTemplate.opsForHash().putAll("authParams", params);
            return new HttpRes(ResEnum.OK);
        } catch (Exception e) {
            return new HttpRes(ResEnum.SERVERERR, null, e.getMessage());
        }
    }

}
