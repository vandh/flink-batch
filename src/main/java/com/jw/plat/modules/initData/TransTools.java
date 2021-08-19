package com.jw.plat.modules.initData;


import com.jw.plat.common.util.Constants;
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
 * @description 将 dw表 add_rtp_ap_invoice_info 和add_rtp_gl_voucher_info 数据转移到业务库中
 * @Date: 2021/7/7 10:22
 */

public class TransTools {

    private static String sourceIp;
    private static String sourcePort;
    private static String sourceDbname;
    private static String sourceUsername;
    private static String sourcePassword;
    private static String sourceSql;

    private static String destinationIp;
    private static String destinationPort;
    private static String destinationDbname;
    private static String destinationUsername;
    private static String destinationPassword;
    private static String destinationSql;


    private static String serviceType;

    private static Set<String> inserTableList;


    @Test
    public void trans(String[] args) throws Exception {
        //初始化数据
        init(args);
        if (serviceType != null && serviceType.equals("oneToOne")) {
            //取数
            Connection connection = getConnection(sourceIp, sourcePort, sourceDbname, sourceUsername, sourcePassword);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sourceSql);
            List<List> list = new ArrayList();
            while (resultSet.next()) {
                //获取列数
                int columnCount = resultSet.getMetaData().getColumnCount();
                List item = new ArrayList();
                for (int i = 0; i < columnCount; i++) {
                    item.add(resultSet.getString(i + 1));
                }
                list.add(item);
            }
            resultSet.close();
            statement.close();
            connection.close();

            //入库
            Connection connectionDestination = getConnection(destinationIp, destinationPort, destinationDbname, destinationUsername, destinationPassword);
            PreparedStatement preparedStatement = connectionDestination.prepareStatement(destinationSql);
            //关闭自动提交
            connectionDestination.setAutoCommit(false);
            for (int j = 0; j < list.size(); j++) {
                List item = list.get(j);
                try {
                    for (int i = 0; i < item.size(); i++) {
                        preparedStatement.setString(i + 1, (String) item.get(i));
                    }
                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    System.out.println(item);
                }
                while (j % 1000 == 0 && j != 0) {
                    preparedStatement.executeBatch();
                    connectionDestination.commit();
                    preparedStatement.clearBatch();
                }
            }
            preparedStatement.executeBatch();
            connectionDestination.commit();
            preparedStatement.clearBatch();
        }



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

    private static void init(String[] args) throws Exception {
        //获取配置文件
//        String propertiesPath = args[0] + File.separator + "conf.properties";
        String propertiesPath = Constants.PROPERTIESPATH;
        System.out.println("conf.properties: " + propertiesPath);
        //通过配置文件获取参数
        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);

        sourceIp = pm.get("sourceIp");
        sourcePort = pm.get("sourcePort");
        sourceDbname = pm.get("sourceDbname");
        sourceUsername = pm.get("sourceUsername");
        sourcePassword = pm.get("sourcePassword");
        sourceSql = pm.get("sourceSql");

        destinationIp = pm.get("destinationIp");
        destinationPort = pm.get("sourcePort");
        destinationDbname = pm.get("sourceDbname");
        destinationUsername = pm.get("sourceUsername");
        destinationPassword = pm.get("sourcePassword");
        destinationSql = pm.get("destinationSql");

        serviceType = pm.get("serviceType");
    }

}
