package com.dawson.bigdata.offlineDw.template;

import com.dawson.bigdata.offlineDw.constant.SparkConfConstant;
import com.dawson.bigdata.offlineDw.utils.SparkSessionUtil;
import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

@Log4j
public abstract class EtlTemplate {

    public void setup() {
        log.info("set up...");

        SparkConf conf = new SparkConf();
        conf.set(SparkConfConstant.SPARK_APP_NAME,
                conf.get(SparkConfConstant.SPARK_APP_NAME, this.getClass().getSimpleName()));
        conf.set(SparkConfConstant.SPARK_MASTER,
                conf.get(SparkConfConstant.SPARK_MASTER, "local[*]"));

        SparkSession spark = SparkSessionUtil.getSession(conf);
        SparkSessionUtil.set(spark);
    }

    public abstract void etl() throws IOException;

    public void cleanup() {
        log.info("clean up...");

        SparkSession spark = SparkSessionUtil.get();
        SparkSessionUtil.closeSparkSession(spark);

        SparkSessionUtil.remove();
    }

}
