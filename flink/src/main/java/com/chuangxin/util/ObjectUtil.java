package com.chuangxin.util;

import com.chuangxin.bean.Ignore;

import java.util.HashMap;
import java.util.Map;

public class ObjectUtil {
    public static Map<String, String> objectToMap(Object obj) throws IllegalAccessException {
        Map<String, String> map = new HashMap<>();
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                Object value = field.get(obj);
                if (value != null && !field.isAnnotationPresent(Ignore.class)) {
                    map.put(field.getName(), value.toString());
                }
            }
            clazz = clazz.getSuperclass();
        }
        return map;
    }
}
