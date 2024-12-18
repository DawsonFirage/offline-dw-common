package com.dawson.bigdata.offlineDw.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkSqlUtil {

    private static final Logger logger = LoggerFactory.getLogger(SparkSqlUtil.class);

    /**
     * 为 SQL 中参数赋值
     * @param executeSql 需要执行的SQL，参数以${}形式传入。e.g. "SELECT ${int_param}, '${string_param}'"
     * @param conditionMap 参数值映射
     * @return 填充参数后的SQL语句
     */
    public static String getExecuteSql(String executeSql, Map<String, Object> conditionMap) {
        if ( null == conditionMap || conditionMap.isEmpty()) {
            return executeSql;
        }

        // 替换SQL中的占位符
        for (Map.Entry<String, Object> entry : conditionMap.entrySet()) {
            String placeholder = "\\$\\{" + entry.getKey() + "}";
            String value = entry.getValue().toString();

//            // 如果值是字符串类型，则需要加上单引号
//            if (entry.getValue() instanceof String) {
//                value = "'" + value + "'";
//            }

            executeSql = executeSql.replaceAll(placeholder, value);
        }

        return executeSql;
    }

    /**
     * 获取SQL需要的参数
     * @param executeSql 需要执行的SQL，参数以${}形式传入。e.g. "SELECT ${int_param}, '${string_param}'"
     * @return 需要填充的参数列表
     */
    public static Set<String> getSqlParam(String executeSql) {
        String regex = "\\$\\{(.*?)}";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(executeSql);

        Set<String> params = new HashSet<>();
        while (matcher.find()) {
            params.add(matcher.group(1));
        }

        return params;
    }

    /**
     * 获取数据全部列插入表b的语句
     * @param sourceTableName 源表表名
     * @param targetDbName 目的数据库名
     * @param targetTableName 目的表名
     * @return 插入SQL
     */
    public static String getInsertStatement(String sourceTableName, String targetDbName, String targetTableName) {
        return "INSERT INTO " + targetDbName + "." + targetTableName + "\n" +
                "SELECT * FROM " + sourceTableName;
    }

    /**
     * 获取数据全部列插入表b的语句
     * @param sourceTableName 源表表名
     * @param targetDbName 目的数据库名
     * @param targetTableName 目的表名
     * @return 插入SQL
     */
    public static String getInsertStatementWithOperateTime(String sourceTableName, String targetDbName, String targetTableName) {
        return "INSERT INTO " + targetDbName + "." + targetTableName + "\n" +
                "SELECT *, now() AS create_time, now() AS update_time FROM " + sourceTableName;
    }

}