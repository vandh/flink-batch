package com.jw.plat.common.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;

import java.sql.Types;

public class DBUtil {
    public static OutputFormat insertMysql2(String sql, int[] types){
        OutputFormat insertMysql = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(Constants.DRIVER)
                .setDBUrl(Constants.URL)
                .setUsername(Constants.USER)
                .setPassword(Constants.PASS)
                .setQuery(sql)
                .setBatchInterval(Constants.BATCHSIZE)
                .setSqlTypes(types)
                .finish();
        return insertMysql;
    }

    public static OutputFormat insertMysql(String sql, int[] types){
        OutputFormat insertMysql = MysqlUtil.buildMysqlUtil()
                .setQuery(sql)
                .setSqlTypes(types)
                .finish();
        return insertMysql;
    }

    public static OutputFormat insertMysql3(String sql, int[] types){
        OutputFormat insertMysql = MysqlUtil.buildMysqlUtil()
                .setQuery(sql.replaceAll("@batch", Constants.batch))
                .setSqlTypes(types)
                .finish();
        return insertMysql;
    }

    public static int[] getSqlTypes(String sql){
        int len = sql.length() - sql.replaceAll("\\?","").length();
        int[] types = new int[len];
        for(int i =0; i<len ; i++) {
            types[i] = Types.VARCHAR;
        }
        return types;
    }
}
