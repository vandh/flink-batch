package com.jw.plat.dealDateController;





import com.jw.plat.common.util.Constants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.sql.*;

import java.util.ArrayList;
import java.util.List;


/**
 * @Author: lijun
 * @description
 * @Date: 2021/6/9 19:37
 */

public class dealOuData {


//    @Value("${ouURL}")
    private  static  String ouURL ="jdbc:mysql://10.242.29.19:10121/rtp_dw_db?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";
    private static String ouUSER ="root";
    private  static String ouPASSWORD ="GA81Da81bO34" ;
    private  static String outSql ="select * from rtp_party_info limit 100 ;" ;

    private  static  String inURL ="jdbc:mysql://127.0.0.1:3306/mysql?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";
    private static String inUSER ="root";
    private  static String inPASSWORD ="123456" ;
    private  static String inSql ="INSERT INTO student2(`name`,`age`,`sex`) VALUES(?,?,?)" ;

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
       /* String propertiesPath = args[0]+ File.separator+"conf.properties";
        System.out.println("conf.properties: "+propertiesPath);
        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);
        String ouURL= pm.get("ouURL");
        String ouUSER= pm.get("ouUSER");
        String ouPASSWORD= pm.get("ouPASSWORD");
        String outSql= pm.get("outSql");

        String inURL= pm.get("inURL");
        String inUSER= pm.get("inUSER");
        String inPASSWORD= pm.get("inPASSWORD");
        String inSql= pm.get("inSql");*/

        System.out.println("=================================读库开始==================================");
        List<List> list =new ArrayList();
        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2。创建连接
        Connection connection = DriverManager.getConnection(ouURL,ouUSER,ouPASSWORD);
        //设置连接时间
        connection.setNetworkTimeout(null, 30*60*1000);
        //3.预编译
        PreparedStatement preparedStatement = connection.prepareStatement(outSql);
        ResultSet resultSet = preparedStatement.executeQuery();
        //Statement statement = connection.createStatement();
        //ResultSet resultSet = statement.executeQuery(outSql);
        while (resultSet.next()){
            // 获取行数
            //int row = execute.getRow();
            // 获取列
            ResultSetMetaData metaData = resultSet.getMetaData();
            //获取列的数量
            int columnCount = metaData.getColumnCount();
            ArrayList arrayList = new ArrayList();
            for (int i = 0; i < columnCount; i++) {
                String str = String.valueOf(resultSet.getObject(i+1));
                //获取列名
                String columnTypeName = metaData.getColumnTypeName(i + 1);
                String catalogName = metaData.getCatalogName(i + 1);
                //获取列的数据
                arrayList.add(str);
            }
            list.add(arrayList);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();

        System.out.println("=================================入库开始==================================");

        //2。创建连接
        Connection connection2 = DriverManager.getConnection(inURL,inUSER,inPASSWORD);
        //设置连接时间
        connection2.setNetworkTimeout(null, 30*60*1000);
        //3.预编译
        PreparedStatement preparedStatement2 = connection2.prepareStatement(inSql);
        connection2.setAutoCommit(false);//手动提交

        for (int i = 0; i < list.size(); i++) {
            List item = list.get(i);
            for (int j = 0; j < item.size(); j++) {
                preparedStatement2.setObject(j+1, String.valueOf(item.get(j)));
            }
            preparedStatement2.addBatch();
            while (i%1000==0&&i!=0){
                preparedStatement2.executeBatch();
                connection2.commit();
                preparedStatement2.clearBatch();
            }

        }
        preparedStatement2.executeBatch();
        connection2.commit();
        preparedStatement2.clearBatch();

        connection2.close();


    }
}

