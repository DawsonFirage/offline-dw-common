package com.dawson.bigdata.offlineDw.dw;

import com.dawson.bigdata.offlineDw.conf.ClickhouseConf;
import com.dawson.bigdata.offlineDw.template.EtlTemplate;
import com.dawson.bigdata.offlineDw.utils.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Test extends EtlTemplate {
    @Override
    public void etl() throws IOException {
        SparkSession spark = SparkSessionUtil.get();

//        ClickhouseConf chConf = ClickhouseConf.init();
        System.setProperty("HADOOP_HOME", "null");
        String hadoopHome = System.getenv("HADOOP_HOME");
        System.out.printf(hadoopHome);

    }
}
