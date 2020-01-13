package com.stevenchennet.net;

import java.util.Properties;

public class Utils {
    public static Properties getKafkaProperties(){
        Properties properties = new Properties();
        properties.put("", "");
        properties.put("", "");
        properties.put("group.id","ABC");
        return properties;
    }
}
