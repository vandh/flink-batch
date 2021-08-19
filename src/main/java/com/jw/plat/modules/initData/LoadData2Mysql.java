package com.jw.plat.modules.initData;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * @author seven
 * @since 07.03.2013
 */
public class LoadData2Mysql {
    String ip;
    String port;
    String dbname;
    String username;
    String password;
    String filepath;
    String table;
    String batch;

    public LoadData2Mysql(String[] args) {
        if(args.length != 8) {
            System.out.println("param must be : %DB %ip %port %dbname %username %passowrd %filepath %table ");
            System.exit(0);
        }
        this.ip = args[1];
        this.port = args[2];
        this.dbname = args[3];
        this.username = args[4];
        this.password = args[5];
        this.filepath = args[6];
        this.table = args[7].toUpperCase();
//        this.batch = args[8];

        if(table.indexOf("GL")!=-1 ||
                table.indexOf("AP")!=-1 ) {
        } else {
            System.out.println("table must be : GL GLAP APPRE APSTD");
            System.exit(0);
        }
    }

    private String getSql() {
        String sql = null;
        switch (table.substring(0, table.length()-2)) {
            case "GL_VOUCHER_INFO":
                return "";
            case "AP_PRE_VOUCHER_INFO":
                return "";
            case "AP_STD_VOUCHER_INFO":
                return "";
            default:
                return sql;
        }
    }

    public void load() {
        Connection conn = null;
        Statement st = null;
        int result = 0;
        //1.加载驱动程序
        try {
            conn = DriverManager.getConnection("jdbc:mysql://"+ip+":"+port+"/"+dbname+"?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0",
                    username, password);
            st = conn.createStatement();

            try {
                st.executeQuery("select 1 from " + table + " limit 1");
            }catch (Exception e) {
                System.out.println("table not exists, create table "+table+" now!");
                st.executeUpdate(getSql());
            }


            File dir = new File(filepath);
            if(!dir.exists()) {
                System.out.println(dir+" not exists!");
                System.exit(0);
            }
            File[] files = dir.listFiles();
            if(files.length==0) {
                System.out.println(dir+" has not file more than one!");
                System.exit(0);
            }
            for(int i=0; i<files.length; i++) {
                long start = System.currentTimeMillis();
                String fpath = files[i].getPath().replaceAll("\\\\","/");
                System.out.println("load data from "+fpath);
                String sql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+table +" character set utf8 fields terminated by '\u0007' LINES TERMINATED BY '\n' ";
                int rows = st.executeUpdate(sql);
                st.clearBatch();
                System.out.println("SQL===>  " + sql );
                System.out.println("importing "+ rows + " rows data into "+table+" cost "+(System.currentTimeMillis()-start)/1000+"s!" );
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                st.close();
                conn.close();
            }catch(SQLException e) {}
        }
    }
}
