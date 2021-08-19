package com.jw.plat.dealDateController;


import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * @Author: lijun
 * @description
 * @Date: 2021/6/24 10:35
 */
public class readDataToFile {
    public static void main(String[] args) throws IOException {
        // 封装数据源(创建集合对象)
        ArrayList<student> array = new ArrayList<student>();
        // 存储字符串数据
        array.add(new student("zhangsan", "18", "boy"));
        array.add(new student("lisi", "20", "boy"));
        array.add(new student("wangwu", "18", "girl"));
        array.add(new student("wangwu", "18", "girl"));


      /*  BufferedReader bufferedReader =new BufferedReader(new FileReader("I:\\a.txt"));
        bufferedReader.readLine();*/


        // 封装目的地
        BufferedWriter bw = new BufferedWriter(new FileWriter("I:\\a.txt"));
        // 遍历
        for (student s : array) {
            // 写出数据
            StringBuffer stringBuffer = new StringBuffer();
            String s2 = stringBuffer.append(s.getName()).append(",").append(s.getAge()).append(",").append(s.getSex()).toString();
            System.out.println(s2);
            bw.write(s2);
            bw.newLine();
            bw.flush();
        }

        // 释放资源
        bw.close();

    }

    @Test
    public void loadDATA() {
        Connection conn = null;
        Statement st = null;
        int result = 0;
        //1.加载驱动程序
        try {
            String table = "student";
            String ip = "127.0.0.1";
            String port = "3306";
            String dbname = "mysql";
            String username = "root";
            String password = "123456";
            conn = DriverManager.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + dbname + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0",
                    username, password);
            st = conn.createStatement();
//            String url = "E:".concat(File.separator).concat("loadDataTest");
            String url = "E:\\loadDataTest";
           // File fpath = new File("E:\\\\testData\\\\flinkData\\\\testFlink\\\\loadDataTest.txt");
            File fpath = new File(url);
            String sql = "LOAD DATA LOCAL INFILE '" + "E:/loadDataTest" + "' IGNORE INTO TABLE " + table + " character set utf8 fields terminated by ',' LINES TERMINATED BY '\n' ";
            System.out.println(sql);
            int rows = st.executeUpdate(sql);
            st.clearBatch();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                st.close();
                conn.close();
            } catch (SQLException e) {
            }
        }

    }


    public static class student {
        private String name;
        private String age;
        private String sex;

        public student(String name, String age, String sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }
    }
}

