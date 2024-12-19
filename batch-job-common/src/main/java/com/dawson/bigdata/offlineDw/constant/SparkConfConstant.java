package com.dawson.bigdata.offlineDw.constant;

public class SparkConfConstant {

    public static final String SPARK_SQL_CATALOG = "spark.sql.catalog";

    public static final String SPARK_APP_NAME = "spark.app.name";

    public static final String SPARK_MASTER = "spark.master";

    /**
     * 生成成功标记位的参数 如果有_success文件 因该成功文件没有数据那样的字段信息 dataX下发时会报错
     * 非 spark. 开头的参数通过 spark conf 传入会不生效，故如果需要通过参数传入，在传入和读取时需要添加 spark. 前缀
     */
    public static final String MAPREDUCE_MARK_SUCCESSFUL_JOBS_FLAG = "mapreduce.fileoutputcommitter.marksuccessfuljobs";

}
