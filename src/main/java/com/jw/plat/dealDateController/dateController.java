package com.jw.plat.dealDateController;


import org.apache.commons.lang3.StringUtils;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: lijun
 * @description
 * @Date: 2021/6/9 19:37
 */

public class dateController {

    public void dealDate() throws IOException {

     BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\apgl\\GLAP-1-20210609"));
//        BufferedReader in = new BufferedReader(new FileReader("I:\\data\\GLAP-1\\2"));

        //BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\APSTD01\\4"));
   //     BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\GLAP\\gl-ap\\GLAP-1\\4"));
//        BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\APSTD01\\3"));
//        BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\APSTD01\\4"));
        String line = in.readLine();
        List list = new ArrayList();
        while (line != null) {
            String[] split = line.split(",");
            list.add(Arrays.asList(split));
            line = in.readLine();
        }
        in.close();
//
        System.out.println(list.size());
    }


    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
       // BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\GLAP-1-20210609"));
        BufferedReader in = new BufferedReader(new FileReader("I:\\flink\\result\\APPRE-1"));
        String line = in.readLine();
        List<List> list = new ArrayList();
        while (line != null) {
            String[] split = line.split(",");
            list.add(Arrays.asList(split));
            line = in.readLine();
        }
        in.close();


        Connection conn = null;
        PreparedStatement prepare =null;
        long start =0L;
        long end = 0L;
        try {
            String URL = "jdbc:mysql://192.168.10.221:3307/db_2?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";
            String USER = "root";
            String PASSWORD = "123456";
            //1.加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
            //2.获得数据库链接
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            start = System.currentTimeMillis();
            //3.通过数据库的连接操作数据库，实现增删改查（使用Statement类）
            //String sql = "insert into apgl(reserver1,reserver2,reserver3,reserver4,reserver5,reserver6,reserver7,reserver8,reserver9,reserver10,reserver11,reserver12,reserver13) values (?,?,?,?,?,?,?,?,?,?,?,?,?) ;";
            String sql = "insert into appre(reserver1,reserver2,reserver3,reserver4,reserver5,reserver6,reserver7,reserver8,reserver9,reserver10,reserver11,reserver12,reserver13,reserver14,reserver15,reserver16,reserver17,reserver18,reserver19,reserver20,reserver21,reserver22,reserver23,reserver24,reserver25,reserver26,reserver27,reserver28,reserver29,reserver30,reserver31,reserver32,reserver33,reserver34,reserver35,reserver36 ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ;";


            prepare = conn.prepareStatement(sql);
            conn.setAutoCommit(false);//手动提交

            for (int a = 0; a < list.size(); a++) {
                List item = list.get(a);
                for (int j = 0; j < item.size(); j++) {
                    prepare.setString(j + 1, String.valueOf(item.get(j)));
                }
                prepare.addBatch();
                if ((a+1) % 1000 == 0) {
                    prepare.executeBatch();
                    prepare.clearBatch();
                    conn.commit();//提交插入数据
                }
            }

            prepare.executeBatch();
            conn.commit();

        } finally {
             end = System.currentTimeMillis();
            System.out.println(end-start);
            prepare.close();
            //关闭资源
            conn.close();
        }


    }
}

