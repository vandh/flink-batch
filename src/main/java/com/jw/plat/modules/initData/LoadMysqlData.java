package com.jw.plat.modules.initData;


import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.*;
import java.util.Date;


/**
 * @author seven
 * @since 07.03.2013
 */
public class LoadMysqlData {
    private Connection conn = null;

    /**
     * load bulk data from InputStream to MySQL
     */
    public int getMysqlConnection_bak(String loadDataSql) throws SQLException {
        conn = DriverManager.getConnection("jdbc:mysql://10.242.29.19:10026/rtp_dw_db?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0",
                "root", "2Fea2f6elW2t");
        //conn = DriverManager.getConnection("jdbc:mysql://192.168.10.221:3311/db_1?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&failOverReadOnly=false",
        //        "root", "123456");
        //6L245r2p7Yrk
        Statement statement = conn.createStatement();// prepareStatement(loadDataSql);
        int result = 0;

        result = statement.executeUpdate(loadDataSql);

        return result;
    }
    public int  getMysqlConnection(String sql)throws SQLException {
        Connection conn = null;
        Statement statement = null;
        int result = 0;
        //1.加载驱动程序
        try {
            conn = DriverManager.getConnection("jdbc:mysql://10.242.29.19:10121/rtp_dw_db?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0",
                    "root", "GA81Da81bO34");
            statement = conn.createStatement();
            result = statement.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            statement.close();
            //关闭资源
            conn.close();
        }
        return result;
    }

    /**
     * 批量处理sql
     */
    public void APExecute(String filePath,String fileNumber) {

        String AP_BATCHES_ALL = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_BATCHES_ALL_"+fileNumber+"' IGNORE INTO TABLE AP_BATCHES_ALL_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_INVOICE_PRE = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_INVOICE_PRE_"+fileNumber+"' IGNORE INTO TABLE ap_invoice_pre_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_INVOICE_STD = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_INVOICE_STD_"+fileNumber+"' IGNORE INTO TABLE ap_invoice_std_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_INVOICE_DIST_PRE = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_INVOICE_DIST_PRE_"+fileNumber+"' IGNORE INTO TABLE ap_invoice_dist_pre_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_INVOICE_DIST_STD = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_INVOICE_DIST_STD_"+fileNumber+"' IGNORE INTO TABLE ap_invoice_dist_std_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_XLA_DIS_LINKS = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_XLA_DIS_LINKS_"+fileNumber+"' IGNORE INTO TABLE AP_XLA_DIS_LINKS_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_XLA_TRX_ENTITIES = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_XLA_TRX_ENTITIES_"+fileNumber+"' IGNORE INTO TABLE AP_XLA_TRX_ENTITIES_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String FND_USER = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_FND_USER_"+fileNumber+"' IGNORE INTO TABLE FND_USER_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String PO_VENDOR_SITES_ALL = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_PO_VENDOR_SITES_ALL_"+fileNumber+"' IGNORE INTO TABLE PO_VENDOR_SITES_ALL_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_XLA_AE_HEADERS = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_XLA_AE_HEADERS_"+fileNumber+"' IGNORE INTO TABLE AP_XLA_AE_HEADERS_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String AP_XLA_AE_LINES = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_AP_XLA_AE_LINES_"+fileNumber+"' IGNORE INTO TABLE AP_XLA_AE_LINES_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String PO_VENDORS = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_PO_VENDORS_"+fileNumber+"' IGNORE INTO TABLE PO_VENDORS_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_CODE_COMBINATIONS = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_CODE_COMBINATIONS_"+fileNumber+"' IGNORE INTO TABLE GL_CODE_COMBINATIONS_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";

        // GL数据
        String GL_JE_BATCHES_POST = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_BATCHES_POST_"+fileNumber+"' IGNORE INTO TABLE GL_JE_BATCHES_POST_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_JE_CATEGORIES_TL = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_CATEGORIES_TL_"+fileNumber+"' IGNORE INTO TABLE GL_JE_CATEGORIES_TL_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_JE_HEADERS_POST = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_HEADERS_POST_"+fileNumber+"' IGNORE INTO TABLE GL_JE_HEADERS_POST_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_JE_LINES_POST = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_LINES_POST_"+fileNumber+"' IGNORE INTO TABLE GL_JE_LINES_POST_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_LEDGERS = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_LEDGERS_"+fileNumber+"' IGNORE INTO TABLE GL_LEDGERS_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String QGL_APPROVE = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_QGL_APPROVE_"+fileNumber+"' IGNORE INTO TABLE QGL_APPROVE_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String QGL_SEQUENCE_VALUE = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_QGL_SEQUENCE_VALUE_"+fileNumber+"' IGNORE INTO TABLE QGL_SEQUENCE_VALUE_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";


        System.out.println("AP_BATCHES_ALL SQL===>  " + AP_BATCHES_ALL );
        LoadMysqlData load = new LoadMysqlData();
        try {
            System.out.println("AP_BATCHES_ALL 开始时间 time  " + new Date() );
            int rows = load.getMysqlConnection(AP_BATCHES_ALL);
            System.out.println(" AP_BATCHES_ALL 结束时间 time  " + new Date() );
            System.out.println("AP_BATCHES_ALL importing " + rows + " rows data into mysql and cost " );

            System.out.println("AP_INVOICE_PRE 开始时间 time  " + new Date() );
            int rows1 = load.getMysqlConnection(AP_INVOICE_PRE);
            System.out.println(" AP_INVOICE_PRE 结束时间 time  " + new Date() );
            System.out.println("AP_INVOICE_PRE importing " + rows1 + " rows data into mysql and cost " );

            System.out.println("AP_INVOICE_STD 开始时间 time  " + new Date() );
            int rows2 = load.getMysqlConnection(AP_INVOICE_STD);
            System.out.println(" AP_INVOICE_STD 结束时间 time  " + new Date() );
            System.out.println("AP_INVOICE_STD importing " + rows2 + " rows data into mysql and cost " );

            System.out.println("AP_INVOICE_DIST_PRE 开始时间 time  " + new Date() );
            int rows3 = load.getMysqlConnection(AP_INVOICE_DIST_PRE);
            System.out.println(" AP_INVOICE_DIST_PRE 结束时间 time  " + new Date() );
            System.out.println("AP_INVOICE_DIST_PRE importing " + rows3 + " rows data into mysql and cost " );

            System.out.println("AP_INVOICE_DIST_STD 开始时间 time  " + new Date() );
            int rows4 = load.getMysqlConnection(AP_INVOICE_DIST_STD);
            System.out.println(" AP_INVOICE_DIST_STD 结束时间 time  " + new Date() );
            System.out.println("AP_INVOICE_DIST_STD importing " + rows4 + " rows data into mysql and cost " );

            System.out.println("AP_XLA_DIS_LINKS 开始时间 time  " + new Date() );
            int rows5 = load.getMysqlConnection(AP_XLA_DIS_LINKS);
            System.out.println(" AP_XLA_DIS_LINKS 结束时间 time  " + new Date() );
            System.out.println("AP_XLA_DIS_LINKS importing " + rows5 + " rows data into mysql and cost " );

            System.out.println("AP_XLA_TRX_ENTITIES 开始时间 time  " + new Date() );
            int rows6 = load.getMysqlConnection(AP_XLA_TRX_ENTITIES);
            System.out.println(" AP_XLA_TRX_ENTITIES 结束时间 time  " + new Date() );
            System.out.println("AP_XLA_TRX_ENTITIES importing " + rows6 + " rows data into mysql and cost " );

            System.out.println("FND_USER 开始时间 time  " + new Date() );
            int rows7 = load.getMysqlConnection(FND_USER);
            System.out.println(" FND_USER 结束时间 time  " + new Date() );
            System.out.println("FND_USER importing " + rows7 + " rows data into mysql and cost " );

            System.out.println("PO_VENDOR_SITES_ALL 开始时间 time  " + new Date() );
            int rows9 = load.getMysqlConnection(PO_VENDOR_SITES_ALL);
            System.out.println(" PO_VENDOR_SITES_ALL 结束时间 time  " + new Date() );
            System.out.println("PO_VENDOR_SITES_ALL importing " + rows9 + " rows data into mysql and cost " );

            System.out.println("AP_XLA_AE_LINES 开始时间 time  " + new Date() );
            int rows10 = load.getMysqlConnection(AP_XLA_AE_LINES);
            System.out.println(" AP_XLA_AE_LINES 结束时间 time  " + new Date() );
            System.out.println("AP_XLA_AE_LINES importing " + rows10 + " rows data into mysql and cost " );

            System.out.println("AP_XLA_AE_HEADERS 开始时间 time  " + new Date() );
            int rows11 = load.getMysqlConnection(AP_XLA_AE_HEADERS);
            System.out.println(" AP_XLA_AE_HEADERS 结束时间 time  " + new Date() );
            System.out.println("AP_XLA_AE_HEADERS importing " + rows11 + " rows data into mysql and cost " );

            System.out.println("PO_VENDORS 开始时间 time  " + new Date() );
            int rows12 = load.getMysqlConnection(PO_VENDORS);
            System.out.println(" PO_VENDORS 结束时间 time  " + new Date() );
            System.out.println("PO_VENDORS importing " + rows12 + " rows data into mysql and cost " );

            System.out.println("GL_CODE_COMBINATIONS 开始时间 time  " + new Date() );
            int rows13 = load.getMysqlConnection(GL_CODE_COMBINATIONS);
            System.out.println(" GL_CODE_COMBINATIONS 结束时间 time  " + new Date() );
            System.out.println("GL_CODE_COMBINATIONS importing " + rows13 + " rows data into mysql and cost " );

            System.out.println("GL_JE_BATCHES_POST 开始时间 time  " + new Date() );
            int rows14 = load.getMysqlConnection(GL_JE_BATCHES_POST);
            System.out.println(" GL_JE_BATCHES_POST 结束时间 time  " + new Date() );
            System.out.println("GL_JE_BATCHES_POST importing " + rows14 + " rows data into mysql and cost " );

            System.out.println("GL_JE_CATEGORIES_TL 开始时间 time  " + new Date() );
            int rows15 = load.getMysqlConnection(GL_JE_CATEGORIES_TL);
            System.out.println(" GL_JE_CATEGORIES_TL 结束时间 time  " + new Date() );
            System.out.println("GL_JE_CATEGORIES_TL importing " + rows15 + " rows data into mysql and cost " );

            System.out.println("GL_JE_HEADERS_POST 开始时间 time  " + new Date() );
            int rows16 = load.getMysqlConnection(GL_JE_HEADERS_POST);
            System.out.println(" GL_JE_HEADERS_POST 结束时间 time  " + new Date() );
            System.out.println("GL_JE_HEADERS_POST importing " + rows16 + " rows data into mysql and cost " );

            System.out.println("GL_JE_LINES_POST 开始时间 time  " + new Date() );
            int rows17 = load.getMysqlConnection(GL_JE_LINES_POST);
            System.out.println(" GL_JE_LINES_POST 结束时间 time  " + new Date() );
            System.out.println("GL_JE_LINES_POST importing " + rows17 + " rows data into mysql and cost " );

            System.out.println("GL_LEDGERS 开始时间 time  " + new Date() );
            int rows18 = load.getMysqlConnection(GL_LEDGERS);
            System.out.println(" GL_LEDGERS 结束时间 time  " + new Date() );
            System.out.println("GL_LEDGERS importing " + rows18 + " rows data into mysql and cost " );

            System.out.println("QGL_APPROVE 开始时间 time  " + new Date() );
            int rows19 = load.getMysqlConnection(QGL_APPROVE);
            System.out.println(" QGL_APPROVE 结束时间 time  " + new Date() );
            System.out.println("QGL_APPROVE importing " + rows19 + " rows data into mysql and cost " );

            System.out.println("QGL_SEQUENCE_VALUE 开始时间 time  " + new Date() );
            int rows20 = load.getMysqlConnection(QGL_SEQUENCE_VALUE);
            System.out.println(" QGL_SEQUENCE_VALUE 结束时间 time  " + new Date() );
            System.out.println("QGL_SEQUENCE_VALUE importing " + rows20 + " rows data into mysql and cost " );


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 批量处理sql
     */
    public void GLExecute(String filePath,String fileNumber) {

        // GL数据
        String GL_JE_BATCHES_POST = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_BATCHES_POST_"+fileNumber+"' IGNORE INTO TABLE GL_JE_BATCHES_POST_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_JE_CATEGORIES_TL = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_CATEGORIES_TL_"+fileNumber+"' IGNORE INTO TABLE GL_JE_CATEGORIES_TL_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_JE_HEADERS_POST = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_HEADERS_POST_"+fileNumber+"' IGNORE INTO TABLE GL_JE_HEADERS_POST_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_JE_LINES_POST = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_JE_LINES_POST_"+fileNumber+"' IGNORE INTO TABLE GL_JE_LINES_POST_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String GL_LEDGERS = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_GL_LEDGERS_"+fileNumber+"' IGNORE INTO TABLE GL_LEDGERS_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String QGL_APPROVE = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_QGL_APPROVE_"+fileNumber+"' IGNORE INTO TABLE QGL_APPROVE_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";
        String QGL_SEQUENCE_VALUE = "LOAD DATA LOCAL INFILE '"+filePath+"/D_20210607_QGL_SEQUENCE_VALUE_"+fileNumber+"' IGNORE INTO TABLE QGL_SEQUENCE_VALUE_"+fileNumber+" character set gbk fields terminated by '\u0007' LINES TERMINATED BY '\u0006' ";


        System.out.println("GL_JE_BATCHES_POST SQL===>  " + GL_JE_BATCHES_POST );
        LoadMysqlData load = new LoadMysqlData();
        try {

            System.out.println("GL_JE_BATCHES_POST 开始时间 time  " + new Date() );
            int rows14 = load.getMysqlConnection(GL_JE_BATCHES_POST);
            System.out.println(" GL_JE_BATCHES_POST 结束时间 time  " + new Date() );
            System.out.println("GL_JE_BATCHES_POST importing " + rows14 + " rows data into mysql and cost " );

            System.out.println("GL_JE_CATEGORIES_TL 开始时间 time  " + new Date() );
            int rows15 = load.getMysqlConnection(GL_JE_CATEGORIES_TL);
            System.out.println(" GL_JE_CATEGORIES_TL 结束时间 time  " + new Date() );
            System.out.println("GL_JE_CATEGORIES_TL importing " + rows15 + " rows data into mysql and cost " );

            System.out.println("GL_JE_HEADERS_POST 开始时间 time  " + new Date() );
            int rows16 = load.getMysqlConnection(GL_JE_HEADERS_POST);
            System.out.println(" GL_JE_HEADERS_POST 结束时间 time  " + new Date() );
            System.out.println("GL_JE_HEADERS_POST importing " + rows16 + " rows data into mysql and cost " );

            System.out.println("GL_JE_LINES_POST 开始时间 time  " + new Date() );
            int rows17 = load.getMysqlConnection(GL_JE_LINES_POST);
            System.out.println(" GL_JE_LINES_POST 结束时间 time  " + new Date() );
            System.out.println("GL_JE_LINES_POST importing " + rows17 + " rows data into mysql and cost " );

            System.out.println("GL_LEDGERS 开始时间 time  " + new Date() );
            int rows18 = load.getMysqlConnection(GL_LEDGERS);
            System.out.println(" GL_LEDGERS 结束时间 time  " + new Date() );
            System.out.println("GL_LEDGERS importing " + rows18 + " rows data into mysql and cost " );

            System.out.println("QGL_APPROVE 开始时间 time  " + new Date() );
            int rows19 = load.getMysqlConnection(QGL_APPROVE);
            System.out.println(" QGL_APPROVE 结束时间 time  " + new Date() );
            System.out.println("QGL_APPROVE importing " + rows19 + " rows data into mysql and cost " );

            System.out.println("QGL_SEQUENCE_VALUE 开始时间 time  " + new Date() );
            int rows20 = load.getMysqlConnection(QGL_SEQUENCE_VALUE);
            System.out.println(" QGL_SEQUENCE_VALUE 结束时间 time  " + new Date() );
            System.out.println("QGL_SEQUENCE_VALUE importing " + rows20 + " rows data into mysql and cost " );


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) throws Exception{
//        String filePath = args[0];
//        String fileNumber = args[1];
//        LoadMysqlData load = new LoadMysqlData();
//        load.APExecute(filePath,fileNumber);
//        System.exit(1);
        String period_name = "2021-01";
        String code = "2021-012021-022021-032021-042021-05";
        System.out.println(code.indexOf(period_name)!=-1);
    }

}
