package com.jw.plat.modules.initData;


import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author seven
 * @since 07.03.2013
 */
public class LoadData2Mysql3 {
    String ip;
    String port;
    String dbname;
    String username;
    String password;
    String filepath;
    String table;
    String batch;

    public LoadData2Mysql3(String[] args) {
        if(args.length != 8) {
            System.out.println("param must be : %DB %ip %port %dbname %username %passowrd %filepath %table ");
            System.exit(0);
        }
        // DB 10.242.29.19 10121 rtp_dw_db root GA81Da81bO34 /data/flink/APSTD-2 AP_STD_VOUCHER_INFO_2
        this.ip = args[1];
        this.port = args[2];
        this.dbname = args[3];
        this.username = args[4];
        this.password = args[5];
        this.filepath = args[6];
        this.table = args[7].toUpperCase();
//        this.batch = args[8];

//        if(table.indexOf("GL")!=-1 ||
//                table.indexOf("AP")!=-1 ) {
//        } else {
//            System.out.println("table must be : GL GLAP APPRE APSTD");
//            System.exit(0);
//        }
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
//                //匹配表明
//                String path = files[i].getPath();
//                String pathDirName= path.split("\\\\")[path.split("\\\\").length-1];
//                if (pathDirName.indexOf("PRE")!=-1)
//                    //批次文件插入到对应 批次的表
//                   table="ADD_AP_PRE_VOUCHER_INFO_"+ pathDirName.substring(pathDirName.indexOf("-")+1,pathDirName.length());
//                    //执行批次文件插入到同一张表
////                    table="ADD_AP_PRE_VOUCHER_INFO";
//                if (pathDirName.indexOf("STD")!=-1)
//                    table="ADD_AP_STD_VOUCHER_INFO_"+ pathDirName.substring(pathDirName.indexOf("-")+1,pathDirName.length());
////                    table="ADD_AP_STD_VOUCHER_INFO";
//                if (pathDirName.indexOf("GL")!=-1)
//                    table="ADD_GL_VOUCHER_INFO_"+ pathDirName.substring(pathDirName.indexOf("-")+1,pathDirName.length()) ;
////                    table="ADD_GL_VOUCHER_INFO";
                if(files[i].isDirectory()) {
                    for(int j =0; j<files[i].listFiles().length; j++) {
                        procFile(files[i].listFiles()[j],st,table);
                    }
                } else {
                    procFile(files[i],st,table);
                }

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

   public void  procFile(File file,Statement st,String table) throws SQLException {
        long start = System.currentTimeMillis();
        String fpath = file.getPath().replaceAll("\\\\","/");
        System.out.println("load data from "+fpath);
        String sql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+table +" character set utf8 fields terminated by '\u0007' LINES TERMINATED BY '\n' ";
        int rows = st.executeUpdate(sql);
        st.clearBatch();
        System.out.println("SQL===>  " + sql );
        System.out.println("importing "+ rows + " rows data into "+table+" cost "+(System.currentTimeMillis()-start)/1000+"s!" );
    }


}
