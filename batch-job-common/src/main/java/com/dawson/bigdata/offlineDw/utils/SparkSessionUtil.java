package com.dawson.bigdata.offlineDw.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionUtil {

    private static final ThreadLocal<SparkSession> sparkThreadLocal = new ThreadLocal<>();

    public static void set(SparkSession spark) {
        sparkThreadLocal.set(spark);
    }

    public static SparkSession get() {
        return sparkThreadLocal.get();
    }

    public static void remove() {
        sparkThreadLocal.remove();
    }

    public static SparkSession getSession(SparkConf conf) {
        SparkSession.Builder builder = SparkSession.builder().config(conf);

        // TODO 根据传入参数创建对应的Session...

        return builder.getOrCreate();
    }

    public static void closeSparkSession(SparkSession spark) {
        if (null != spark) {
            spark.stop();
        }
    }

}
