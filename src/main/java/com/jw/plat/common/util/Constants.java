package com.jw.plat.common.util;

import com.jw.plat.modules.ap.AP_INVOICE_PRE;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

public class Constants {
    public static String DRIVER; // = "com.mysql.cj.jdbc.Driver";
    public static String URL; // = "jdbc:mysql://192.168.10.221:3307/db_2";
    public static String USER; // = "root";
    public static String PASS; // = "123456";
    public static int BATCHSIZE = 5000;

    public static String  PROPERTIESPATH;

    public static final String LF = "\u0006";  //ACK
    public static final String DEL = "\u0007"; //BEL
    public static final String A10 = "\u0010"; //LF
    public static final String A13 = "\u0013"; //CR 回车符
    public static final String A34 =  "\u0002";  //"
    public static final String A39 = "\u0039"; //,
    public static final String HH = "\n";  //ACK
    public static final String DH = ","; //BEL
    public final static String JH = "-";

    public static Map<String, Tuple3<BaseExecute, String,String>> BIZMAP;

    public static final String GL_JE_BATCHES_POST="GL_JE_BATCHES_POST";
    public static final String GL_JE_HEADERS_POST="GL_JE_HEADERS_POST";
    public static final String GL_JE_LINES_POST="GL_JE_LINES_POST";
    public static final String GL_JE_SOURCES_TL="GL_JE_SOURCES_TL";
    public static final String GL_CODE_COMBINATIONS="GL_CODE_COMBINATIONS";
    public static final String GL_LEDGERS="GL_LEDGERS";
    public static final String GL_JE_CATEGORIES_TL="GL_JE_CATEGORIES_TL";
    public static final String GL = "GL";
    public static final String GL2 = "GL2";
    public static final String GL7 = "GL7";
    public static final String GLPRE = "GLPRE";
    public static final String GLSTD = "GLSTD";
    public static final String RA = "RA" ;  //ACK
    public static final String AR = "AR" ;

    public static final String AP_BATCHES_ALL="AP_BATCHES_ALL";
    public static final String AP_INVOICE_STD="AP_INVOICE_STD";
    public static final String AP_INVOICE_PRE="AP_INVOICE_PRE";
    public static final String AP_INVOICE_DIST_STD="AP_INVOICE_DIST_STD";
    public static final String AP_INVOICE_DIST_PRE="AP_INVOICE_DIST_PRE";
    public static final String PO_VENDOR_SITES_ALL="PO_VENDOR_SITES_ALL";
    public static final String AP_XLA_AE_HEADERS="AP_XLA_AE_HEADERS";
    public static final String AP_XLA_AE_LINES="AP_XLA_AE_LINES";
    public static final String AP_XLA_DIS_LINKS="AP_XLA_DIS_LINKS";
    public static final String AP_XLA_TRX_ENTITIES="AP_XLA_TRX_ENTITIES";
    public static final String AP_GL_IMPORT_REFERENCES="AP_GL_IMPORT_REFERENCES";
    public static final String APPRE = "APPRE";
    public static final String APSTD = "APSTD";
    public static final String APSTD2 = "APSTD2";
    public static final String APSTD3 = "APSTD3";
    public static final String APSTD4 = "APSTD4";
    public static final String GLAP = "GLAP";
    public static final String QGL_SEQUENCE_VALUE="QGL_SEQUENCE_VALUE";
    public static final String QGL_APPROVE="QGL_APPROVE";
    public static final String PO_VENDORS="PO_VENDORS";
    public static final String FND_USER="FND_USER";

    public static String PATH;  //所有批文件的路径，其下没有子目录
    public static String OUTPATH;  //所有批文件的路径，其下没有子目录
    public static String GLSQL="INSERT INTO add_gl_voucher_info_@batch (je_batch_id, BATCHE_name, creation_date, created_by, default_period_name, je_header_id, je_category, je_source, period_name, header_name, currency_code, actual_flag, je_batch_id_1, batch_description, currency_conversion_rate, je_header_id_1, e_line_num, ledger_id, code_combination_id, period_name_1, effective_date, creation_date_1, created_by_1, accounted_dr, accounted_cr, description, gl_sl_link_id, gl_sl_link_table, reference_9, je_category_name, je_language, user_je_category_name, je_source_name, js_language, user_je_source_name, je_header_id_2, sequence_value, last_update_date, ods_creation_date, code_combination_id_1, chart_of_accounts_id, summary_flag, segment1, segment2, segment3, segment4, segment5, segment6, segment7, segment8, segment9, user_id, user_name, last_update_date_1, je_header_id_3, post_person, post_date, invoice_no, project_no) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";  //抽取的AP的sql;  //抽取的总账sql
    public static String APSQL;  //抽取的AP的sql
    public static String GLAPSQL;
    public final static String OP_FILE = "FILE";  //输出类型，FILE=文件，DB=数据库
    public final static String OP_DB = "DB";  //输出类型，FILE=文件，DB=数据库
    public static String BATCHDATE;  //抽取的日期
    public static String GLCHARSET;  //字符集
    public static String batch;  //文件批次，从1，2，3，...，8


    public static String APPRESQL="INSERT INTO add_ap_pre_voucher_info_@batch(batch_id, batch_name, invoice_id, vendor_id, invoice_num, vendor_site_id, source, description, created_by, attribute4, org_id, gl_date, ACCOUNTING_DATE, INVOICE_DISTRIBUTION_ID, VENDOR_SITE_CODE, VENDOR_NAME, ap_SEGMENT1, APPLICATION_ID, AE_HEADER_ID, AE_LINE_NUM, SOURCE_DISTRIBUTION_TYPE, ENTITY_ID, CODE_COMBINATION_ID, ACCOUNTED_DR, ACCOUNTED_CR, USSGL_TRANSACTION_CODE, ENTITY_CODE, segment1, segment2, segment3, segment4, segment5, segment6, segment7, segment8, segment9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";  //抽取的AP的sql
    public static String APSTDSQL="INSERT INTO add_ap_std_voucher_info_@batch(batch_id, batch_name, invoice_id, vendor_id, invoice_num, vendor_site_id, source, description, created_by, attribute4, org_id, gl_date, ACCOUNTING_DATE, INVOICE_DISTRIBUTION_ID, VENDOR_SITE_CODE, VENDOR_NAME, ap_SEGMENT1, APPLICATION_ID, AE_HEADER_ID, AE_LINE_NUM, SOURCE_DISTRIBUTION_TYPE, ENTITY_ID, CODE_COMBINATION_ID, ACCOUNTED_DR, ACCOUNTED_CR, USSGL_TRANSACTION_CODE, ENTITY_CODE, segment1, segment2, segment3, segment4, segment5, segment6, segment7, segment8, segment9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";  //抽取的AP的sql
    public static String RASQL="";
    public static String ARSQL="";

    //应收表
    public static final String RA_CUSTOMER_TRX_ALL="RA_CUSTOMER_TRX_ALL";
    public static final String RA_CUSTOMER_TRX_LINES_ALL="RA_CUSTOMER_TRX_LINES_ALL";
    public static final String RA_CUST_TRX_LINE_GL_DIST="RA_CUST_TRX_LINE_GL_DIST";
    public static final String HZ_CUST_ACCOUNTS="HZ_CUST_ACCOUNTS";



    //收款表
    public static final String AR_CASH_RECEIPTS_ALL="AR_CASH_RECEIPTS_ALL";
    public static final String AR_CASH_RECEIPT_HISTORY_ALL="AR_CASH_RECEIPT_HISTORY_ALL";
    public static final String AR_DISTRIBUTIONS_ALL="AR_DISTRIBUTIONS_ALL";
    public static final String AR_CUSTOMER_TRX_ALL="AR_CUSTOMER_TRX_ALL";
    public static final String HZ_PARTIES="HZ_PARTIES";
    public static final String XLA_EVENTS="XLA_EVENTS";





}
