package com.jw.plat.modules.initData;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author seven
 * @since 07.03.2013
 */
public class LoadData2Mysql2 {
    String ip;
    String port;
    String dbname;
    String username;
    String password;
    String filepath;
    String table ;
//   I:/flink/dw01  1  GBK 2021-01 GLAP 1  FILE I:/flink/result  ip   port  dbname  username  password
    public LoadData2Mysql2(String[] args) {
        /*if(args.length != 8) {
            System.out.println("param must be : %DB %ip %port %dbname %username %passowrd %filepath %table ");
            System.exit(0);
        }*/
        this.ip = args[9];
        this.port = args[10];
        this.dbname = args[11];
        this.username = args[12];
        this.password = args[13];
        this.filepath = args[7];
        this.table = args[5].toUpperCase();
//        this.batch = args[8];

        if(table.indexOf("GL")!=-1 || table.indexOf("APGL")!=-1 ||
                table.indexOf("APPRE")!=-1 || table.indexOf("APSTD")!=-1) {
        } else {
            System.out.println("table must be : GL GLAP APPRE APSTD");
            System.exit(0);
        }
    }

    private String getSql() {
        String sql = null;
        switch (table.substring(0, table.length()-2)) {
            case "GL":
                return "    create table "+ table +"(\n" +
                        "   je_batch_id    VARCHAR(50),\n" +
                        "   name    VARCHAR(50),\n" +
                        "   creation_date    VARCHAR(50),\n" +
                        "   created_by    VARCHAR(50),\n" +
                        "   default_period_name    VARCHAR(50),\n" +
                        "   org_id    VARCHAR(50),\n" +
                        "   je_header_id    VARCHAR(50),\n" +
                        "   je_category    VARCHAR(50),\n" +
                        "   je_source    VARCHAR(50),\n" +
                        "   period_name    VARCHAR(50),\n" +
                        "   name    VARCHAR(50),\n" +
                        "   currency_code    VARCHAR(50),\n" +
                        "   actual_flag    VARCHAR(50),\n" +
                        "   je_batch_id    VARCHAR(50),\n" +
                        "   description    VARCHAR(50),\n" +
                        "   currency_conversion_rate    VARCHAR(50),\n" +
                        "   je_header_id    VARCHAR(50),\n" +
                        "   je_line_num    VARCHAR(50),\n" +
                        "   ledger_id    VARCHAR(50),\n" +
                        "   code_combination_id    VARCHAR(50),\n" +
                        "   period_name    VARCHAR(50),\n" +
                        "   effective_date    VARCHAR(50),\n" +
                        "   creation_date    VARCHAR(50),\n" +
                        "   created_by    VARCHAR(50),\n" +
                        "   accounted_dr    VARCHAR(50),\n" +
                        "   accounted_cr    VARCHAR(50),\n" +
                        "   description    VARCHAR(50),\n" +
                        "   gl_sl_link_id    VARCHAR(50),\n" +
                        "   gl_sl_link_table    VARCHAR(50),\n" +
                        "   reference_9    VARCHAR(50),\n" +
                        "   je_category_name    VARCHAR(50),\n" +
                        "   language    VARCHAR(50),\n" +
                        "   user_je_category_name    VARCHAR(50),\n" +
                        "   je_source_name    VARCHAR(50),\n" +
                        "   language    VARCHAR(50),\n" +
                        "   user_je_source_nam    VARCHAR(50),\n" +
                        "   je_header_id    VARCHAR(50),\n" +
                        "   sequence_value    VARCHAR(50),\n" +
                        "   last_update_date    VARCHAR(50),\n" +
                        "   ods_creation_date    VARCHAR(50),\n" +
                        "   code_combination_id    VARCHAR(50),\n" +
                        "   chart_of_accounts_id    VARCHAR(50),\n" +
                        "   summary_flag    VARCHAR(50),\n" +
                        "   segment1    VARCHAR(50),\n" +
                        "   segment2    VARCHAR(50),\n" +
                        "   segment3    VARCHAR(50),\n" +
                        "   segment4    VARCHAR(50),\n" +
                        "   segment5    VARCHAR(50),\n" +
                        "   segment6    VARCHAR(50),\n" +
                        "   segment7    VARCHAR(50),\n" +
                        "   segment8    VARCHAR(50),\n" +
                        "   segment9    VARCHAR(50),\n" +
                        "   user_id    VARCHAR(50),\n" +
                        "   user_name    VARCHAR(50),\n" +
                        "   last_update_date    VARCHAR(50),\n" +
                        "   je_header_id    VARCHAR(50),\n" +
                        "   post_person    VARCHAR(50),\n" +
                        "   post_date		   VARCHAR(50) )ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            case "APPRE":
                return "     create table "+ table +"(\n" +
                        "      batch_id   VARCHAR(50),\n" +
                        "      batch_name VARCHAR(50),\n" +
                        "      invoice_id    VARCHAR(50),\n" +
                        "      vendor_id     VARCHAR(50),\n" +
                        "      invoice_num   VARCHAR(50),\n" +
                        "      vendor_site_id  VARCHAR(50),\n" +
                        "      source        VARCHAR(50),\n" +
                        "      description   VARCHAR(2000),\n" +
                        "      created_by    VARCHAR(50),\n" +
                        "      attribute4    VARCHAR(50),\n" +
                        "      org_id        VARCHAR(50),\n" +
                        "      gl_date    VARCHAR(50),\n" +
                        "      ACCOUNTING_DATE       VARCHAR(50),\n" +
                        "      INVOICE_DISTRIBUTION_ID  VARCHAR(50),\n" +
                        "      VENDOR_SITE_CODE      VARCHAR(50),\n" +
                        "      VENDOR_NAME   VARCHAR(50),\n" +
                        "      SEGMENT1 VARCHAR ( 50 ),\n" +
                        "      APPLICATION_ID    VARCHAR(50),\n" +
                        "      AE_HEADER_ID   VARCHAR(50),\n" +
                        "      AE_LINE_NUM   VARCHAR(50),\n" +
                        "      SOURCE_DISTRIBUTION_TYPE   VARCHAR(50),\n" +
                        "      ENTITY_ID   VARCHAR(50),\n" +
                        "      CODE_COMBINATION_ID   VARCHAR(50),\n" +
                        "      ACCOUNTED_DR   VARCHAR(50),\n" +
                        "      ACCOUNTED_CR   VARCHAR(50),\n" +
                        "      USSGL_TRANSACTION_CODE   VARCHAR(50),\n" +
                        "      ENTITY_CODE       VARCHAR(50),\n" +
                        "      segment1_1 VARCHAR(50),\n" +
                        "      segment2 VARCHAR(50),\n" +
                        "      segment3 VARCHAR(50),\n" +
                        "      segment4 VARCHAR(50),\n" +
                        "      segment5 VARCHAR(50),\n" +
                        "      segment6 VARCHAR(50),\n" +
                        "      segment7 VARCHAR(50),\n" +
                        "      segment8 VARCHAR(50),\n" +
                        "     segment9 VARCHAR(50) )ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            case "APSTD":
                return "     create table "+ table +"(\n" +
                        "     batch_id   VARCHAR(50),\n" +
                        "     batch_name VARCHAR(50),\n" +
                        "     invoice_id    VARCHAR(50),\n" +
                        "     vendor_id     VARCHAR(50),\n" +
                        "     invoice_num   VARCHAR(50),\n" +
                        "     vendor_site_id  VARCHAR(50),\n" +
                        "     source        VARCHAR(50),\n" +
                        "     description   VARCHAR(2000),\n" +
                        "     created_by    VARCHAR(50),\n" +
                        "     attribute4    VARCHAR(50),\n" +
                        "     org_id        VARCHAR(50),\n" +
                        "     gl_date    VARCHAR(50),\n" +
                        "     ACCOUNTING_DATE       VARCHAR(50),\n" +
                        "     INVOICE_DISTRIBUTION_ID  VARCHAR(50),\n" +
                        "     VENDOR_SITE_CODE      VARCHAR(50),\n" +
                        "     VENDOR_NAME   VARCHAR(50),\n" +
                        "     SEGMENT1   VARCHAR(50),\n" +
                        "     APPLICATION_ID    VARCHAR(50),\n" +
                        "     AE_HEADER_ID   VARCHAR(50),\n" +
                        "     AE_LINE_NUM   VARCHAR(50),\n" +
                        "     SOURCE_DISTRIBUTION_TYPE   VARCHAR(50),\n" +
                        "     ENTITY_ID   VARCHAR(50),\n" +
                        "     CODE_COMBINATION_ID   VARCHAR(50),\n" +
                        "     ACCOUNTED_DR   VARCHAR(50),\n" +
                        "     ACCOUNTED_CR   VARCHAR(50),\n" +
                        "     USSGL_TRANSACTION_CODE   VARCHAR(50),\n" +
                        "     ENTITY_CODE       VARCHAR(50),\n" +
                        "     segment1_1 VARCHAR(50),\n" +
                        "     segment2 VARCHAR(50),\n" +
                        "     segment3 VARCHAR(50),\n" +
                        "     segment4 VARCHAR(50),\n" +
                        "     segment5 VARCHAR(50),\n" +
                        "     segment6 VARCHAR(50),\n" +
                        "     segment7 VARCHAR(50),\n" +
                        "     segment8 VARCHAR(50),\n" +
                        "     segment9 VARCHAR(50) )ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            case "APGL":
                return "CREATE TABLE "+table+" (\n" +
                        "  je_batch_id1 varchar(500) DEFAULT NULL,\n" +
                        "  name1 varchar(500) DEFAULT NULL,\n" +
                        "  default_period_name varchar(500) DEFAULT NULL,\n" +
                        "  org_id varchar(500) DEFAULT NULL,\n" +
                        "  je_header_id varchar(500) DEFAULT NULL,\n" +
                        "  je_source varchar(500) DEFAULT NULL,\n" +
                        "  period_name varchar(500) DEFAULT NULL,\n" +
                        "  name2 varchar(500) DEFAULT NULL,\n" +
                        "  je_batch_id2 varchar(500) DEFAULT NULL,\n" +
                        "  je_line_num varchar(500) DEFAULT NULL,\n" +
                        "  sequence_value varchar(500) DEFAULT NULL,\n" +
                        "  reference_5 varchar(500) DEFAULT NULL\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
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
                String sql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+table +" character set utf8 fields terminated by ',' LINES TERMINATED BY '\n' ";
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
