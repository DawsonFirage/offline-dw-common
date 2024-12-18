package com.dawson.bigdata.offlineDw.conf;

import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Data
public class ClickhouseConf {

    private String host;
    private String port;
    private String dbName;
    private String username;
    private String password;

    private ClickhouseConf(String host, String port, String dbName, String username, String password) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
    }

    public static ClickhouseConf init() throws IOException {
        Properties properties = new Properties();

        InputStream input = ClickhouseConf.class.getClassLoader().getResourceAsStream("clickhouse.properties");

        properties.load(input);

        return new ClickhouseConf(properties.getProperty("host"),
                properties.getProperty("port"),
                properties.getProperty("dbName"),
                properties.getProperty("username"),
                properties.getProperty("password"));

    }

}
