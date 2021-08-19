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
 * @description 将 dw表 add_rtp_ap_invoice_info 和add_rtp_gl_voucher_info 数据转移到业务库中
 * @Date: 2021/7/7 10:22
 */

public class TransDwToService {

    //DB 10.242.29.19 10198 rtp_dw_db root j67XRxOY2440 /data/flinkadd/202106153 ADD_AP_STD_VOUCHER_INFO_1
    private static String dwip;
    private static String dwport;
    private static String dwdbname;
    private static String dwusername;
    private static String dwpassword;

    private static String mycatIp;
    private static String mycatPort;
    private static String mycatDbname;
    private static String mycatUsername;
    private static String mycatPassword;

    private static Set<String> inserTableList = new HashSet<String>();


    private static String dwaptable = "add_rtp_ap_invoice_info";
    private static String dwgltable = "add_rtp_gl_voucher_info";


    @Test
    public static  void trans(String[] args) throws Exception {
        //初始化数据
        init(args);
        for (String table : inserTableList) {
            Connection connection = getConnection(dwip, dwport, dwdbname, dwusername, dwpassword);
            String sql = " select *  from  " + table ;
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            List<List> list = new ArrayList();
            //列数
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {

                List item = new ArrayList();
                for (int i = 0; i < columnCount; i++) {
                    item.add(resultSet.getString(i + 1));
                }
                list.add(item);
            }
            resultSet.close();
            statement.close();
            connection.close();

            System.out.println("===============================================");
            Connection connection2 = getConnection(mycatIp, mycatPort, mycatDbname, mycatUsername, mycatPassword);
//            Connection connection2 = getConnection(dwip, dwport, "dw_2", dwusername, dwpassword);
            String sql2 = null;

            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String deleteGlSql = "delete from rtp_gl_voucher_info where create_data='" + date + "'";
            String deleteApSql = "delete from rtp_ap_invoice_info where create_data='" + date + "'";
            Statement statement1 = connection2.createStatement();

            if (table.equals("add_rtp_gl_voucher_info")) {
                //todo 时间修改  修改sql
                sql2 = "INSERT INTO rtp_gl_voucher_info (ID, COMP_ID, ORG_ID, PERIOD_NAME, SOURCE_SYSTEM, SOURCE_SYSTEM_TITLE, BATCH_NAME, CATEGORY, MAKEVOUCHER_DATE, POSTING_DATE, MAKEVOUCHER_NAME, ACCOUNT_COMBINE_CODE, ACCOUNT_COMBINE_NAME, CO_SEG_CODE, CO_SEG_NAME, COST_SEG_CODE, COST_SEG_NAME, BAND_SEG_CODE, BAND_SEG_NAME, ACCOUNT_SEG_CODE, ACCOUNT_SEG_NAME, IC_SEG_CODE, IC_SEG_NAME, PROJECT_SEG_CODE, PROJECT_SEG_NAME, CLIENT_SEG_CODE, CLIENT_SEG_NAME, RESERVED_SEG_CODE1, RESERVED_SEG_NAME1, RESERVED_SEG_CODE2, RESERVED_SEG_NAME2, VOUCHER_NUMBER, DR_AMOUNT, CR_AMOUNT, CURRENCY, CURRENCY_TITLE, EXCHANGE_RATE, FOREIGN_AMOUNT, COST_INDEX_CODE, COST_INDEX_NAME, VOUCHER_SUMMARY, JOURNAL_NAME, LINE_NUMBER, SUMMARY, I_PERIOD_NAME, RESERVED_1, RESERVED_2, RESERVED_3, RESERVED_4, RESERVED_5, RESERVED_6,create_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?); ";
                int i1 = statement1.executeUpdate(deleteGlSql);
                System.out.println("删掉历史数据rtp_gl_voucher_info："+i1);
            }
            if (table.equals("add_rtp_ap_invoice_info")) {
                sql2 = "INSERT INTO rtp_ap_invoice_info (COMP_ID, ORG_ID, PERIOD_NAME, POSTING_DATE, SOURCE_SYSTEM, SOURCE_SYSTEM_TITLE, BATCH_NAME, BUSINESS_BILL_NUMBER, VENDOR_NO, VENDOR_NAME, VENDOR_SITE, INVOICE_NUMBER, ACCOUNT_COMBINE_CODE, ACCOUNT_COMBINE_NAME, CO_SEG_CODE, CO_SEG_NAME, COST_SEG_CODE, COST_SEG_NAME, BAND_SEG_CODE, BAND_SEG_NAME, ACCOUNT_SEG_CODE, ACCOUNT_SEG_NAME, IC_SEG_CODE, IC_SEG_NAME, PROJECT_SEG_CODE, PROJECT_SEG_NAME, CLIENT_SEG_CODE, CLIENT_SEG_NAME, RESERVED_SEG_CODE1, RESERVED_SEG_NAME1, RESERVED_SEG_CODE2, RESERVED_SEG_NAME2, VOUCHER_NUMBER, DR_AMOUNT, CR_AMOUNT, CURRENCY, CURRENCY_TITLE, EXCHANGE_RATE, FOREIGN_AMOUNT, COST_INDEX_CODE, COST_INDEX_NAME, MAKEINV_NAME, DESCRIPTION, I_PERIOD_NAME, RESERVED_1, RESERVED_2, RESERVED_3, RESERVED_4, RESERVED_5, RESERVED_6, GROUP_ID, SECONDARY_COMP_ID,create_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,? );";
                int i2 = statement1.executeUpdate(deleteApSql);
                System.out.println("删掉历史数据rtp_gl_voucher_info："+i2);
            }
            PreparedStatement statement2 = connection2.prepareStatement(sql2);
            connection2.setAutoCommit(false);
            for (int j = 0; j < list.size(); j++) {
                List item = list.get(j);
                try {
                    for (int i = 0; i < item.size(); i++) {
                        statement2.setString(i + 1, (String) item.get(i));
                    }
                    statement2.addBatch();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    System.out.println(item);
                }
                while (j % 1000 == 0 && j != 0) {
                    statement2.executeBatch();
                    connection2.commit();
                    statement2.clearBatch();
                }

            }
            statement1.close();
            statement2.executeBatch();
            connection2.commit();
            statement2.clearBatch();
            connection2.close();
        }


    }

    //不需要代理
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

    //代理
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

    private static void init(String [] args) throws Exception {
        //获取配置文件
//        String propertiesPath = "E:\\项目资料\\04.IBM关联交易\\conifg\\conf.properties";
//        String propertiesPath = args[0] + File.separator + "conf.properties";
        String propertiesPath = Constants.PROPERTIESPATH;
        System.out.println("conf.properties: " + propertiesPath);
        //通过配置文件获取参数
        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);
        dwip = pm.get("dwip");
        dwport = pm.get("dwport");
        dwdbname = pm.get("dwdbname");
        dwusername = pm.get("dwusername");
        dwpassword = pm.get("dwpassword");

        inserTableList.add(pm.get("dwaptable"));
        inserTableList.add(pm.get("dwgltable"));

        mycatIp = pm.get("mycatIp");
        mycatPort = pm.get("mycatPort");
        mycatDbname = pm.get("mycatDbname");
        mycatUsername = pm.get("mycatUsername");
        mycatPassword = pm.get("mycatPassword");
    }

    @Test
    public void test123() throws SQLException {
        Connection connection2 = getConnection(mycatIp, mycatPort, mycatDbname, mycatUsername, mycatPassword);
        PreparedStatement preparedStatement = connection2.prepareStatement("select * from rtp_gl_voucher_info where COMP_ID='51'");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            Object object = resultSet.getObject(0);
        }

    }
}
