package com.dawson.bigdata.offlineDw.template;

import com.dawson.bigdata.offlineDw.constant.SparkConfConstant;
import com.dawson.bigdata.offlineDw.utils.SparkSessionUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

@Log4j2
public abstract class EtlTemplate {

    public void setup() {
        log.info("set up...");

        SparkConf conf = new SparkConf();
        conf.set(SparkConfConstant.SPARK_APP_NAME,
                conf.get(SparkConfConstant.SPARK_APP_NAME, this.getClass().getSimpleName()));
        conf.set(SparkConfConstant.SPARK_MASTER,
                conf.get(SparkConfConstant.SPARK_MASTER, "local[*]"));

        SparkSession spark = SparkSessionUtil.getSession(conf);

        String jobSuccessFlag = SparkConfConstant.MAPREDUCE_MARK_SUCCESSFUL_JOBS_FLAG;
        // 设置写文件默认不产生 _success 文件
        spark.sparkContext()
                .hadoopConfiguration()
                .set(jobSuccessFlag, conf.get("spark." + jobSuccessFlag, "false"));

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
