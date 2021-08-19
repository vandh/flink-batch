package com.jw.plat.demo;

public class Utils {
    public static String DRIVER = "com.mysql.cj.jdbc.Driver";
    public static String URL = "jdbc:mysql://192.168.10.221:3307/db_2";
    public static String USER = "root";
    public static String PASS = "123456";
    public static int BATCHSIZE = 5000;
    //CREATE TABLE test2(c0 INT NOT NULL AUTO_INCREMENT PRIMARY KEY, c1 INT,c2 FLOAT,c3 INT, c4 INT, c5 FLOAT, c6 INT,c7 INT,c8 INT, c9 INT,c10 INT,c11 INT);
    public static String T_SQL = "insert into test2(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) values (?,?,?,?,?,?,?,?,?,?,?)";  //
    //CREATE TABLE t_data2(id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, c1 VARCHAR(10),c2 INT ,c3 INT, c4 INT);
    public static String T_SQL2 = "insert into t_data2(c1,c2,c3,c4) values (?,?,?,?)";  //
}
