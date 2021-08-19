package com.jw.plat.modules.initData;


import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @Author: lijun
 * @description 分页读数据 到文件  然后 load到数据库
 * @Date: 2021/7/7 10:22
 */

public class TransTools2 {

    private static String sourceIp;
    private static String sourcePort;
    private static String sourceDbname;
    private static String sourceUsername;
    private static String sourcePassword;
    private static String sourceSql;
    private static String sourceCountSql;

    private static String destinationIp;
    private static String destinationPort;
    private static String destinationDbname;
    private static String destinationUsername;
    private static String destinationPassword;
    private static String destinationSql;


    private static String serviceType;

    private static Set<String> inserTableList;


    @Test
//    public void trans(String[] args) throws Exception {
    public void trans() throws Exception {
        //初始化数据
//        init(args);
        init();

        //取数
        Connection sourceConnection = getConnection(sourceIp, sourcePort, sourceDbname, sourceUsername, sourcePassword);
        Statement sourceStatement = sourceConnection.createStatement();
        ResultSet sourceResultCount = sourceStatement.executeQuery(sourceCountSql);
        ResultSet sourceResultSet = null;

        //入库
        Connection destinationConnection = getConnection(sourceIp, sourcePort, sourceDbname, sourceUsername, sourcePassword);
        //Connection connectionDestination = getConnection(destinationIp, destinationPort, destinationDbname, destinationUsername, destinationPassword);
        PreparedStatement destinationStatement = destinationConnection.prepareStatement(destinationSql);

        int count = 0;
        int size = 100;
        while (sourceResultCount.next()) {
            Number number = (Number) sourceResultCount.getObject(1);
            count = number.intValue();
        }

        int page = (int) Math.ceil(count * 1.0 / size);

        //分页查询
        for (int p = 0; p < page; p++) {
            int start = p * size;
            String sql = sourceSql + " limit " + start + " , " + size;
            sourceResultSet = sourceStatement.executeQuery(sql);
            List<List> list = new ArrayList();
            while (sourceResultSet.next()) {
                //获取列数
                int columnCount = sourceResultSet.getMetaData().getColumnCount();
                List item = new ArrayList();
                for (int i = 0; i < columnCount; i++) {
                    String columnValue = sourceResultSet.getString(i + 1);
                    //处理特需字符\
                    if (columnValue.indexOf("\\") != -1) {
                        columnValue = columnValue.replace("\\", " ");
                    }
                    item.add(columnValue);
                }
                list.add(item);
            }

            //入库
            destinationStatement = destinationConnection.prepareStatement(destinationSql);
            //关闭自动提交
            destinationConnection.setAutoCommit(false);
            for (int j = 0; j < list.size(); j++) {
                List item = list.get(j);
                try {
                    for (int i = 0; i < item.size(); i++) {
                        destinationStatement.setString(i + 1, (String) item.get(i));
                    }
                    destinationStatement.addBatch();
                } catch (SQLException e) {
                    System.out.println(item);
                }
                while (j % 1000 == 0 && j != 0) {
                    destinationStatement.executeBatch();
                    destinationConnection.commit();
                    destinationStatement.clearBatch();
                }
            }
            destinationStatement.executeBatch();
            destinationConnection.commit();
            destinationStatement.clearBatch();

        }
        //source关闭连接
        sourceResultCount.close();
        sourceResultSet.close();
        sourceStatement.close();
        sourceConnection.close();
        //destination关闭连接
        destinationStatement.close();
        destinationConnection.close();
    }


    public static Connection getConnection(String ip, String port, String dbname, String username, String password) {
//		System.setProperty("java.net.useSystemProxies","true");
        Properties p = new Properties();
        //p.setProperty("java.net.useSystemProxies", "true");
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

    //    private static void init(String[] args) throws Exception {
    private static void init() throws Exception {
        //获取配置文件
//        String propertiesPath = args[0] + File.separator + "conf.properties";

        String propertiesPath = "E:\\项目资料\\04.IBM关联交易\\conifg\\conf.properties";
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

        serviceType = pm.get("serviceType");
    }


    public static void main(String[] args) {
//        int count =2001;
//        int page = (int) Math.ceil(count*1.0/1000);
        String sql = "select count（*） from invoice limit " + 0 + "," + 100;
        System.out.println(sql);
    }

}
