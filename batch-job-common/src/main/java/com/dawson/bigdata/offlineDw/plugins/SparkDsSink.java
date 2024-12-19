package com.dawson.bigdata.offlineDw.plugins;

import com.dawson.bigdata.offlineDw.utils.SparkSessionUtil;
import com.dawson.bigdata.offlineDw.utils.SparkSqlUtil;
import org.apache.spark.sql.*;

public class SparkDsSink {

    /**
     * 将 Dataset 写出到文件系统
     * @param dataSet 需要写出的DataSet
     * @param outputPath Spark临时视图名称
     */
    public static void toDFS(Dataset<Row> dataSet, String outputPath) {
        dataSet.coalesce(1)
                .write()
                .option("header", true)
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .csv(outputPath);
    }

    /**
     * 当前 Dataset 写出到HDFS
     * @param dataSet 需要写出的DataSet
     * @param outputPath Spark临时视图名称
     */
    public static void toHDFS(Dataset<Row> dataSet, String outputPath) {
        System.setProperty("HADOOP_USER_NAME", "admin");

        dataSet.coalesce(1)
                .write()
                .option("header", true)
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .csv(outputPath);
    }

    /**
     * 当前 Dataset 写出数据库
     * @param dataSet 需要写出的DataSet
     * @param targetDbName 目标数据库
     * @param targetTableName 目标表
     */
    public static void toDatabase(Dataset<Row> dataSet, String targetDbName, String targetTableName) throws AnalysisException {
        // TODO 当前只支持设置对应 DB 的 Catalog 后写入。应支持从不同源读后向不同源写。
        SparkSession spark = SparkSessionUtil.get();

        String sourceTable = targetTableName + "_tmp";
        dataSet.createTempView(sourceTable);

        spark.sql(SparkSqlUtil.getInsertStatement(sourceTable, targetDbName, targetTableName));
    }

}
