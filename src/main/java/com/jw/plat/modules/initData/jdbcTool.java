package com.jw.plat.modules.initData;


import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/**
 * @Author: lijun
 * @description 分页读数据 到文件  然后 load到数据库
 * @Date: 2021/7/7 10:22
 */

//先删全量表  在插入
public class jdbcTool {

    private static String sourceIp;
    private static String sourcePort;
    private static String sourceDbname;
    private static String sourceUsername;
    private static String sourcePassword;
    private static String sourceSql;
    private static String sourceCountSql;
    private static String sourceSqlAppre;
    private static String sourceSqlApstd;
    private static String sourceSqlGl;

    private static String destinationIp;
    private static String destinationPort;
    private static String destinationDbname;
    private static String destinationUsername;
    private static String destinationPassword;
    private static String destinationSql;
    private static String destinationSqlAppre;
    private static String destinationSqlApstd;
    private static String destinationSqlGl;


    private static String serviceType;

    private static Set<String> inserTableList;


    @Test
//    public void trans(String[] args) throws Exception {
    public static void trans(String[] args) throws Exception {
        //初始化数据 TODO
//        init(args);
        //创建连接


        Connection sourceConnection = getConnectionDL("10.238.25.109", "10072", "rtp_dw_db", "root", "evBKIA27vUap");


        String SglSql = "";
        excutesql(sourceConnection, SglSql);


        //关闭连接
        sourceConnection.close();


    }

    private static void excutesql(Connection sourceConnection, String sourceSql) throws SQLException {

        //取数jdbc连接
        Statement sourceStatement = sourceConnection.createStatement();
        int count = sourceStatement.executeUpdate(sourceSql);
        System.out.println("变更条数" + count);


        sourceStatement.close();


    }


    public static Connection getConnectionDL(String ip, String port, String dbname, String username, String password) {
        Properties p = new Properties();
        p.setProperty("socksProxyHost", "192.168.10.210");
        p.setProperty("socksProxyPort", "1080");
        p.setProperty("user", username);
        p.setProperty("password", password);
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbname + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0";
        Connection conn = null;
        int result = 0;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, p);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }

    public static Connection getConnection(String ip, String port, String dbname, String username, String password) {
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbname + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0";
        Connection conn = null;
        int result = 0;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
//            conn = DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }

    //    private static void init(String[] args) throws Exception {
    private static void init(String[] args) throws Exception {
        //获取配置文件
        String propertiesPath = args[0] + File.separator + "conf.properties";
//        String propertiesPath = "E:\\项目资料\\04.IBM关联交易\\conifg\\conf.properties";
        System.out.println("conf.properties: " + propertiesPath);
        //通过配置文件获取参数
        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);

        sourceIp = pm.get("sourceIp");
        sourcePort = pm.get("sourcePort");
        sourceDbname = pm.get("sourceDbname");
        sourceUsername = pm.get("sourceUsername");
        sourcePassword = pm.get("sourcePassword");
        sourceSql = pm.get("sourceSql");
        sourceCountSql = pm.get("sourceCountSql");

        destinationIp = pm.get("destinationIp");
        destinationPort = pm.get("sourcePort");
        destinationDbname = pm.get("sourceDbname");
        destinationUsername = pm.get("sourceUsername");
        destinationPassword = pm.get("sourcePassword");
        destinationSql = pm.get("destinationSql");

        sourceSqlAppre = pm.get("sourceSqlAppre");
        sourceSqlApstd = pm.get("sourceSqlApstd");
        sourceSqlGl = pm.get("sourceSqlGl");

        destinationSqlAppre = pm.get("destinationSqlAppre");
        destinationSqlApstd = pm.get("destinationSqlApstd");
        destinationSqlGl = pm.get("destinationSqlGl");

        serviceType = pm.get("serviceType");
    }

    public static void main(String[] args) {
//        int count =2001;
//        int page = (int) Math.ceil(count*1.0/1000);
        String sql = "select count（*） from invoice limit " + 0 + "," + 100;
        System.out.println(sql);
    }

}
