package com.dawson.bigdata.offlineDw.dw;

import com.dawson.bigdata.offlineDw.constant.PathConstant;
import com.dawson.bigdata.offlineDw.plugins.SparkDsSink;
import com.dawson.bigdata.offlineDw.plugins.SparkSourceSetter;
import com.dawson.bigdata.offlineDw.template.EtlTemplate;
import com.dawson.bigdata.offlineDw.utils.SparkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Test extends EtlTemplate {
    @Override
    public void etl() throws IOException {
        SparkSession spark = SparkSessionUtil.get();

//        SparkSourceSetter.setClickhouseCatalog(spark);
        Dataset<Row> resultDs = spark.sql(executeSql);

        SparkDsSink.toDFS(resultDs, PathConstant.DEFAULT_LOCAL_OUTPUT_PATH);
    }

    private final String executeSql = "SELECT 'This is a Spark test Job...' AS col_a";

}
