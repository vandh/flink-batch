package com.jw.plat.modules.initData;


import com.jw.plat.common.util.Constants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * @Author: lijun
 * @description 分页读数据 到文件  然后 load到数据库
 * @Date: 2021/7/7 10:22
 */

//先删全量表  在插入
public class TransDwAddToAll {

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
        init(args);
        //创建连接
        Connection sourceConnection = getConnection(sourceIp, sourcePort, sourceDbname, sourceUsername, sourcePassword);
        Connection destinationConnection = getConnection(sourceIp, sourcePort, sourceDbname, sourceUsername, sourcePassword);
        //循环
       //appre
        for (int k = 1; k <9; k++) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String deleteSql = "delete from ap_pre_voucher_info_"+k+" where create_data='" + date + "'";
            String sourceCountSql="SELECT count(*) from add_ap_pre_voucher_info_"+k;
            String SappreSql = sourceSqlAppre.replace("#{batch}",String.valueOf(k));
            String DappreSql = destinationSqlAppre.replace("#{batch}",String.valueOf(k));
            excutesql(sourceConnection,destinationConnection,SappreSql,DappreSql, deleteSql, sourceCountSql);

        }
        //apstd
        for (int k = 1; k <9; k++) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String deleteSql = "delete from ap_std_voucher_info_"+k+" where create_data='" + date + "'";
            String sourceCountSql="SELECT count(*) from add_ap_pre_voucher_info_"+k;
            String SapstdSql = sourceSqlApstd.replace("#{batch}",String.valueOf(k));
            String DapstdSql = destinationSqlApstd.replace("#{batch}",String.valueOf(k));
//            sourceSqlApstd = sourceSqlApstd.replace("#{batch}",String.valueOf(k));
//            destinationSqlApstd = destinationSqlApstd.replace("#{batch}",String.valueOf(k));
            excutesql(sourceConnection,destinationConnection,SapstdSql,DapstdSql,deleteSql, sourceCountSql);

        }
        //gl
        for (int k = 1; k <9; k++) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String deleteSql = "delete from gl_voucher_info_"+k+" where create_data='" + date + "'";
            String sourceCountSql="SELECT count(*) from add_ap_pre_voucher_info_"+k;
//            sourceSqlGl = sourceSqlGl.replace("#{batch}",String.valueOf(k));
//            destinationSqlGl = destinationSqlGl.replace("#{batch}",String.valueOf(k));
            String SglSql = sourceSqlGl.replace("#{batch}",String.valueOf(k));
            String DglSql = destinationSqlGl.replace("#{batch}",String.valueOf(k));
            excutesql(sourceConnection,destinationConnection,SglSql,DglSql, deleteSql, sourceCountSql);

        }
        //关闭连接
        sourceConnection.close();
        destinationConnection.close();




    }

    private static void excutesql(Connection sourceConnection,Connection destinationConnection,String sourceSql,String destinationSql,String deleteSql,String sourceCountSql) throws SQLException {
        System.out.println(sourceSql);
        System.out.println(destinationSql);
        //取数jdbc连接
        Statement sourceStatement = sourceConnection.createStatement();
        //入库jdbc连接
        PreparedStatement destinationStatement = destinationConnection.prepareStatement(destinationSql);
        //入库之前，先删掉全库当天日期增量的数据
        PreparedStatement preparedStatement = destinationConnection.prepareStatement(deleteSql);
        System.out.println("删除增量语句"+deleteSql);
        int deleteCount = preparedStatement.executeUpdate();
        System.out.println("删掉数据条数："+deleteCount);

        ResultSet sourceResultCount = sourceStatement.executeQuery(sourceCountSql);
        System.out.println(sourceCountSql);
        ResultSet sourceResultSet = null;
        int count = 0;
        int size = 1000;
        while (sourceResultCount.next()) {
            Number number = (Number) sourceResultCount.getObject(1);
            count = number.intValue();
            System.out.println("count:"+count);
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
                    if (Objects.isNull(columnValue)){
                        item.add("");
                        continue;
                    }
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
            /*for (int j = 0; j < list.size(); j++) {
                List item = list.get(j);
                try {
                    for (int i = 0; i < item.size(); i++) {
                        destinationStatement.setString(i + 1, (String) item.get(i));
                    }
                    destinationStatement.executeUpdate();
                } catch (SQLException e) {
                    System.out.println(item);
                }

            }*/
            
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
        //destination关闭连接
        destinationStatement.close();

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
            conn = DriverManager.getConnection(url,username,password);
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
//        String propertiesPath = args[0] + File.separator + "conf.properties";
        String propertiesPath = Constants.PROPERTIESPATH;
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
