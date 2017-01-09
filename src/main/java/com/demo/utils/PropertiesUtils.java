package com.demo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by guang_chen on 2016/11/28.
 */
public class PropertiesUtils {

    private static final String fileName = "config.properties";
    private static Properties properties = new Properties();

    static {
        properties = loadAllProperties(fileName);
    }

    public static String getPropertyByName(String key) {
        String value = properties.getProperty(key);
        return value;
    }

    public static Properties loadAllProperties(String fileName) {
        InputStream in = null;
        try {
            in = PropertiesUtils.class.getClassLoader().getResourceAsStream(fileName);
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
