package com.dawson.bigdata.offlineDw.plugins;

import com.dawson.bigdata.offlineDw.conf.ClickhouseConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import xenon.clickhouse.ClickHouseCatalog;

import java.io.IOException;

public class SparkSourceSetter {

    /**
     * 使 SparkSession 使用 Clickhouse catalog
     * @param spark Spark Session
     */
    public static void setClickhouseCatalog(SparkSession spark) throws IOException {
        ClickhouseConf clickhouseConf = ClickhouseConf.init();

        spark.conf().set("spark.sql.catalog.clickhouse", ClickHouseCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.clickhouse.host", clickhouseConf.getHost());
        spark.conf().set("spark.sql.catalog.clickhouse.port", clickhouseConf.getPort());
        spark.conf().set("spark.sql.catalog.clickhouse.database", clickhouseConf.getDbName());
        spark.conf().set("spark.sql.catalog.clickhouse.user", clickhouseConf.getUsername());
        spark.conf().set("spark.sql.catalog.clickhouse.password", clickhouseConf.getPassword());
        spark.catalog().setCurrentCatalog("clickhouse");
    }

    public Dataset<Row> getFromJson(SparkSession spark, String jsonPath) {
        return spark.read().json(jsonPath).cache();
    }

}
