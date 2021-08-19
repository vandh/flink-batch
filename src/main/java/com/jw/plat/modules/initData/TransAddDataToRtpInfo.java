package com.jw.plat.modules.initData;

import com.jw.plat.common.util.Constants;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @Author: lijun
 * @description 将 dw中24张增量表 生成 add_rtp_ap_invoice_info 和add_rtp_gl_voucher_info
 * @Date: 2021/7/7 10:22
 */

public class TransAddDataToRtpInfo {

    private static String dwip;
    private static String dwport;
    private static String dwdbname;
    private static String dwusername;
    private static String dwpassword;

    private static String appredropSql;
    private static String apstddropSql;
    private static String gldropSql;
    private static String glcreateSql;
    private static String apprecreateSql;
    private static String apstdcreateSql;
    private static String apstdinsertSql;
    private static String appreinsertSql;
    private static String glinsertSql;


    @Test
    public static void trans(String[] args) throws Exception {
        //初始化数据
        init(args);
        Connection connection = getConnection(dwip, dwport, dwdbname, dwusername, dwpassword);
        Statement statement = connection.createStatement();
        //插入数据之前，先删掉当天日期的数据"
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String glsql = "delete from add_rtp_gl_voucher_info where create_data='" + date + "'";
        String apsql = "delete from add_rtp_ap_invoice_info where create_data='" + date + "'";
        int deleteGl = statement.executeUpdate(glsql);
        statement.clearBatch();
        System.out.println("add_rtp_gl_voucher_info表删掉历史数据" + deleteGl);
        int deleteAp = statement.executeUpdate(apsql);
        System.out.println("add_rtp_ap_invoice_info表删掉历史数据" + deleteAp);
        statement.clearBatch();

        //执行前清空rtp增量表
        statement.executeUpdate("truncate table add_rtp_gl_voucher_info");
        statement.executeUpdate("truncate table add_rtp_ap_invoice_info");
        for (int i = 0; i < 8; i++) {
            //插入之前truncate 表
            statement.executeUpdate("truncate table add_gl_voucher_info_"+String.valueOf(i+1));
            try {
                statement.executeUpdate(gldropSql);
            } catch (SQLException e) {
                System.out.println("gl_voucher_info_11 表已经删除，无须再删");
            }
            statement.clearBatch();
            //设置每个批次
            glcreateSql = glcreateSql.replace("#{batch}", String.valueOf(i + 1));
            statement.executeUpdate(glcreateSql);
            statement.clearBatch();
            //插入数据
            statement.executeUpdate(glinsertSql);
            statement.clearBatch();

        }
        for (int i = 0; i < 8; i++) {
            //插入之前truncate 表
            statement.executeUpdate("truncate table add_ap_pre_voucher_info_"+String.valueOf(i+1));
            try {
                statement.executeUpdate(appredropSql);
            } catch (SQLException e) {
                System.out.println("ap_pre_voucher_info_11表已经删除，无须再删");
            }
            statement.clearBatch();
            //设置每个批次
            apprecreateSql = apprecreateSql.replace("#{batch}", String.valueOf(i + 1));
            statement.executeUpdate(apprecreateSql);
            statement.clearBatch();
            //插入数据
            statement.executeUpdate(appreinsertSql);
            statement.clearBatch();

        }

        for (int i = 0; i < 8; i++) {
            //插入之前truncate 表
            statement.executeUpdate("truncate table add_ap_std_voucher_info_"+String.valueOf(i+1));
            try {
                statement.executeUpdate(apstddropSql);
            } catch (SQLException e) {
                System.out.println("ap_std_voucher_info_11 表已经删除，无须再删");
            }
            statement.clearBatch();
            //设置每个批次
            apstdcreateSql = apstdcreateSql.replace("#{batch}", String.valueOf(i + 1));
            statement.executeUpdate(apstdcreateSql);
            statement.clearBatch();
            //插入数据
            statement.executeUpdate(apstdinsertSql);
            statement.clearBatch();
        }
        //更新compid
        statement.executeUpdate("update add_rtp_ap_invoice_info a set CO_SEG_NAME = (select ouname from ou_per_realsion b  where substring_index(b.ounum, '.', 1) = a.CO_SEG_CODE )");
        statement.executeUpdate("update add_rtp_ap_invoice_info a set COMP_ID = (select comp_id from ou_per_realsion b  where substring_index(b.ounum, '.', 1) = a.CO_SEG_CODE )");

        statement.executeUpdate("update add_rtp_gl_voucher_info a set CO_SEG_NAME = (select ouname from ou_per_realsion b  where substring_index(b.ounum, '.', 1) = a.CO_SEG_CODE )");
        statement.executeUpdate("update add_rtp_gl_voucher_info a set COMP_ID = (select comp_id from ou_per_realsion b  where substring_index(b.ounum, '.', 1) = a.CO_SEG_CODE )");

        //将add_rtp 插入到全量的rtp中
        //删掉重复的多余数据 1。和rtp重复  2.和rtp不重复但自身重复
        statement.executeUpdate("delete from add_rtp_gl_voucher_info where id not in ( select id from (select * from add_rtp_gl_voucher_info where (je_batch_id, je_header_id, e_line_num) not in (select je_batch_id, je_header_id, e_line_num from rtp_gl_voucher_info)) w ) ");
        statement.executeUpdate("delete from add_rtp_ap_invoice_info where id not in ( select id from (select * from add_rtp_ap_invoice_info where (batch_id, ae_header_id, ae_line_num) not in (select batch_id, ae_header_id, ae_line_num from rtp_ap_invoice_info)) w )");

        statement.executeUpdate(" delete from add_rtp_ap_invoice_info where ID in(   select ID from (select * FROM add_rtp_ap_invoice_info a WHERE (a.batch_id,a.ae_header_id,a.ae_line_num) IN (SELECT batch_id,ae_header_id,ae_line_num FROM add_rtp_ap_invoice_info GROUP BY batch_id,ae_header_id,ae_line_num HAVING count(*) > 1)   AND ID NOT IN (SELECT min(ID) FROM add_rtp_ap_invoice_info GROUP BY batch_id,ae_header_id,ae_line_num HAVING count(*) > 1)) w ) ");
        statement.executeUpdate("delete from add_rtp_gl_voucher_info where ID in(   select ID from (select * FROM add_rtp_gl_voucher_info a WHERE (a.je_batch_id,a.je_header_id,a.e_line_num) IN (SELECT je_batch_id,je_header_id,e_line_num FROM add_rtp_gl_voucher_info GROUP BY je_batch_id,je_header_id,e_line_num HAVING count(*) > 1)   AND ID NOT IN (SELECT min(ID) FROM add_rtp_gl_voucher_info GROUP BY je_batch_id,je_header_id,e_line_num HAVING count(*) > 1)) w )");

        //将add_rtp 插入到rtp表中
        statement.executeUpdate("insert into rtp_ap_invoice_info_old_2 (COMP_ID, ORG_ID, PERIOD_NAME, POSTING_DATE, SOURCE_SYSTEM, SOURCE_SYSTEM_TITLE, BATCH_NAME, BUSINESS_BILL_NUMBER, VENDOR_NO, VENDOR_NAME, VENDOR_SITE, INVOICE_NUMBER, ACCOUNT_COMBINE_CODE, ACCOUNT_COMBINE_NAME, CO_SEG_CODE, CO_SEG_NAME, COST_SEG_CODE, COST_SEG_NAME, BAND_SEG_CODE, BAND_SEG_NAME, ACCOUNT_SEG_CODE, ACCOUNT_SEG_NAME, IC_SEG_CODE, IC_SEG_NAME, PROJECT_SEG_CODE, PROJECT_SEG_NAME, CLIENT_SEG_CODE, CLIENT_SEG_NAME, RESERVED_SEG_CODE1, RESERVED_SEG_NAME1, RESERVED_SEG_CODE2, RESERVED_SEG_NAME2, VOUCHER_NUMBER, DR_AMOUNT, CR_AMOUNT, CURRENCY, CURRENCY_TITLE, EXCHANGE_RATE, FOREIGN_AMOUNT, COST_INDEX_CODE, COST_INDEX_NAME, MAKEINV_NAME, DESCRIPTION, I_PERIOD_NAME, RESERVED_1, RESERVED_2, RESERVED_3, RESERVED_4, RESERVED_5, RESERVED_6, group_id, SECONDARY_COMP_ID, create_data, batch_id, ae_header_id, ae_line_num) select COMP_ID, ORG_ID, PERIOD_NAME, POSTING_DATE, SOURCE_SYSTEM, SOURCE_SYSTEM_TITLE, BATCH_NAME, BUSINESS_BILL_NUMBER, VENDOR_NO, VENDOR_NAME, VENDOR_SITE, INVOICE_NUMBER, ACCOUNT_COMBINE_CODE, ACCOUNT_COMBINE_NAME, CO_SEG_CODE, CO_SEG_NAME, COST_SEG_CODE, COST_SEG_NAME, BAND_SEG_CODE, BAND_SEG_NAME, ACCOUNT_SEG_CODE, ACCOUNT_SEG_NAME, IC_SEG_CODE, IC_SEG_NAME, PROJECT_SEG_CODE, PROJECT_SEG_NAME, CLIENT_SEG_CODE, CLIENT_SEG_NAME, RESERVED_SEG_CODE1, RESERVED_SEG_NAME1, RESERVED_SEG_CODE2, RESERVED_SEG_NAME2, VOUCHER_NUMBER, DR_AMOUNT, CR_AMOUNT, CURRENCY, CURRENCY_TITLE, EXCHANGE_RATE, FOREIGN_AMOUNT, COST_INDEX_CODE, COST_INDEX_NAME, MAKEINV_NAME, DESCRIPTION, I_PERIOD_NAME, RESERVED_1, RESERVED_2, RESERVED_3, RESERVED_4, RESERVED_5, RESERVED_6, group_id, SECONDARY_COMP_ID, create_data, batch_id, ae_header_id, ae_line_num  from add_rtp_ap_invoice_info");
        statement.executeUpdate("INSERT INTO rtp_gl_voucher_info (COMP_ID, ORG_ID, PERIOD_NAME, SOURCE_SYSTEM, SOURCE_SYSTEM_TITLE, BATCH_NAME, CATEGORY, MAKEVOUCHER_DATE, POSTING_DATE, MAKEVOUCHER_NAME, ACCOUNT_COMBINE_CODE, ACCOUNT_COMBINE_NAME, CO_SEG_CODE, CO_SEG_NAME, COST_SEG_CODE, COST_SEG_NAME, BAND_SEG_CODE, BAND_SEG_NAME, ACCOUNT_SEG_CODE, ACCOUNT_SEG_NAME, IC_SEG_CODE, IC_SEG_NAME, PROJECT_SEG_CODE, PROJECT_SEG_NAME, CLIENT_SEG_CODE, CLIENT_SEG_NAME, RESERVED_SEG_CODE1, RESERVED_SEG_NAME1, RESERVED_SEG_CODE2, RESERVED_SEG_NAME2, VOUCHER_NUMBER, DR_AMOUNT, CR_AMOUNT, CURRENCY, CURRENCY_TITLE, EXCHANGE_RATE, FOREIGN_AMOUNT, COST_INDEX_CODE, COST_INDEX_NAME, VOUCHER_SUMMARY, JOURNAL_NAME, LINE_NUMBER, SUMMARY, I_PERIOD_NAME, RESERVED_1, RESERVED_2, RESERVED_3, RESERVED_4, RESERVED_5, GROUP_ID, SECONDARY_COMP_ID, RESERVED_8, RESERVED_6, create_data, je_batch_id, je_header_id, e_line_num, id) select  COMP_ID, ORG_ID, PERIOD_NAME, SOURCE_SYSTEM, SOURCE_SYSTEM_TITLE, BATCH_NAME, CATEGORY, MAKEVOUCHER_DATE, POSTING_DATE, MAKEVOUCHER_NAME, ACCOUNT_COMBINE_CODE, ACCOUNT_COMBINE_NAME, CO_SEG_CODE, CO_SEG_NAME, COST_SEG_CODE, COST_SEG_NAME, BAND_SEG_CODE, BAND_SEG_NAME, ACCOUNT_SEG_CODE, ACCOUNT_SEG_NAME, IC_SEG_CODE, IC_SEG_NAME, PROJECT_SEG_CODE, PROJECT_SEG_NAME, CLIENT_SEG_CODE, CLIENT_SEG_NAME, RESERVED_SEG_CODE1, RESERVED_SEG_NAME1, RESERVED_SEG_CODE2, RESERVED_SEG_NAME2, VOUCHER_NUMBER, DR_AMOUNT, CR_AMOUNT, CURRENCY, CURRENCY_TITLE, EXCHANGE_RATE, FOREIGN_AMOUNT, COST_INDEX_CODE, COST_INDEX_NAME, VOUCHER_SUMMARY, JOURNAL_NAME, LINE_NUMBER, SUMMARY, I_PERIOD_NAME, RESERVED_1, RESERVED_2, RESERVED_3, RESERVED_4, RESERVED_5, GROUP_ID, SECONDARY_COMP_ID, RESERVED_8, RESERVED_6, create_data, je_batch_id, je_header_id, e_line_num, id  from add_rtp_gl_voucher_info");

        statement.close();
        connection.close();

    }


    public static Connection getConnection(String ip, String port, String dbname, String username, String password) {
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbname + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0";
        Connection conn = null;
        int result = 0;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }

    public static Connection getConnectionDL(String ip, String port, String dbname, String username, String password) {
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

        appredropSql = pm.get("appredropSql");
        apstddropSql = pm.get("apstddropSql");
        gldropSql = pm.get("gldropSql");
        glcreateSql = pm.get("glcreateSql");
        apprecreateSql = pm.get("apprecreateSql");
        apstdcreateSql = pm.get("apstdcreateSql");
        apstdinsertSql = pm.get("apstdinsertSql");
        appreinsertSql = pm.get("appreinsertSql");
        glinsertSql = pm.get("glinsertSql");

    }

    public static void main(String[] args) {

        File file = new File("I:\\flink\\dw01\\conf.properties");
        boolean exists = file.exists();
        System.out.println(exists);
    }
}
