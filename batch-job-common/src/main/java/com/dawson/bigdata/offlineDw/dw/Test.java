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

        spark.sql("Select 'spark sql running...'")
                .show();

        ClickhouseConf clickhouseConf = ClickhouseConf.init();

        System.out.println(clickhouseConf.getHost());
    }
}
