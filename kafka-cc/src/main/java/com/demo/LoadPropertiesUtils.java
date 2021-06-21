package com.demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: cs
 * @Date: 2021/4/16 5:20 下午
 * @Desc:
 */
public class LoadPropertiesUtils {
    private static final Properties properties = new Properties();

    static {
        InputStream inputStream = LoadPropertiesUtils.class.getClassLoader()
                .getResourceAsStream("kafka2.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//
//    public static Properties getProperties() {
//
//
//        return properties;
//    }

//    public static void main(String[] args) {
//        System.out.println(LoadPropertiesUtils.getProperties().getProperty("topic"));
//    }
}
