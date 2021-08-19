package com.jw.plat.modules.ap;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.row.Tuples;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

/**
 * AP_XLA_DIS_LINKS：AE_LINE_NUM不同，所以开始会有重复记录
 * AP_XLA_AE_LINES: AE_LINE_NUM关联后，重复记录去掉了
 */
public class APExecute_STD2 implements BaseExecute {

    public  DataSet<Row> run(ExecutionEnvironment env) throws RuntimeException{
        DataSet<Row> ds = null;
        try {
            ds= AP_BATCHES_ALL.proc(env).join(AP_INVOICE_STD.proc(env))
                    .where(0)
                    .equalTo(6)
                    .with(new JoinFunction<Tuple2<String, String>,
                            Tuple11<String, String, String, String, String, String, String, String, String, String, String>,
                            Tuple12< String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple2<String, String> t1,
                                Tuple11<String, String, String, String, String, String, String, String, String, String, String> t2) throws Exception {
                            return Tuples.comb(t1,t2, new int[]{0,1,2,3,4,5,7,8,9,10},12);
                        }
                    })
            .join(AP_INVOICE_DIST_STD.proc(env))
            .where(2)
            .equalTo(1)
            .with(new JoinFunction<Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>,
                    Tuple3<String, String, String>,
                    Row >() {
                @Override
                public Row join(
                        Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> t1,
                        Tuple3<String, String, String> t2) throws Exception {
                        return Rows.create(t1,t2, new int[]{0,2});
//                    return Tuples.comb(t1,t2, new int[]{0,2},14);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    ;

        return ds;
//        return null;
//        .writeAsText("d:/flink/ap.txt");
//        .output(
//                DBUtil.insertMysql(Constants.APSQL,DBUtil.getSqlTypes(Constants.APSQL)
//        ));
//                .writeAsCsv("d:/flink/ap.txt", Constants.HH, Constants.DH);
    }

    /**
     AP_BATCHES_ALL
     0     batch_id
     1     batch_name

     AP_INVOICE_PRE
     2     invoice_id
     3      vendor_id
     4     * invoice_num
     5     * vendor_site_id
     6     * source
     7     * description
     * batch_id       x
     8     * created_by
     9     * attribute4
     10     * org_id
     11     * gl_date

     AP_INVOICE_DIST_PRE
     12     * ACCOUNTING_DATE
     * INVOICE_ID     x
     13     * INVOICE_DISTRIBUTION_ID

     PO_VENDOR_SITES_ALL
     * VENDOR_SITE_ID       x
     14     * VENDOR_SITE_CODE

     PO_VENDORS
     * VENDOR_ID   x
     15     * VENDOR_NAME
     16     * SEGMENT1

     AP_XLA_DIS_LINKS
     17     * APPLICATION_ID
     18     * AE_HEADER_ID
     19     * AE_LINE_NUM
     20     * SOURCE_DISTRIBUTION_TYPE
     * SOURCE_DISTRIBUTION_ID_NUM_1   x

     AP_XLA_AE_HEADERS
     * AE_HEADER_ID    x
     * APPLICATION_ID  x
     21     * ENTITY_ID

     AP_XLA_AE_LINES
     * AE_HEADER_ID   x
     * AE_LINE_NUM    x
     * APPLICATION_ID   x
     22     * CODE_COMBINATION_ID
     23     * ACCOUNTED_DR
     24     * ACCOUNTED_CR
     25     * USSGL_TRANSACTION_CODE

     AP_XLA_TRX_ENTITIES
     * ENTITY_ID        x
     26     * ENTITY_CODE
     * SOURCE_ID_INT_1  x
     * SECURITY_ID_INT_1  x

     GL_CODE_COMBINATIONS
     * code_combination_id  x
     * chart_of_accounts_id x
     * summary_flag         x
     27     * segment1
     * segment2
     * segment3
     * segment4
     * segment5
     * segment6
     * segment7
     * segment8
     35     * segment9

     FND_USER
     * user_id  x
     * user_name x
     * last_update_date	 x

     create table t_data5(
     batch_id   VARCHAR(50),
     batch_name VARCHAR(50),
     invoice_id    VARCHAR(50),
     vendor_id     VARCHAR(50),
     invoice_num   VARCHAR(50),
     vendor_site_id  VARCHAR(50),
     source        VARCHAR(50),
     description   VARCHAR(2000),
     created_by    VARCHAR(50),
     attribute4    VARCHAR(50),
     org_id        VARCHAR(50),
     gl_date    VARCHAR(50),
     ACCOUNTING_DATE       VARCHAR(50),
     INVOICE_DISTRIBUTION_ID  VARCHAR(50),
     VENDOR_SITE_CODE      VARCHAR(50),
     VENDOR_NAME   VARCHAR(50),
     SEGMENT1   VARCHAR(50),
     APPLICATION_ID    VARCHAR(50),
     AE_HEADER_ID   VARCHAR(50),
     AE_LINE_NUM   VARCHAR(50),
     SOURCE_DISTRIBUTION_TYPE   VARCHAR(50),
     ENTITY_ID   VARCHAR(50),
     CODE_COMBINATION_ID   VARCHAR(50),
     ACCOUNTED_DR   VARCHAR(50),
     ACCOUNTED_CR   VARCHAR(50),
     USSGL_TRANSACTION_CODE   VARCHAR(50),
     ENTITY_CODE       VARCHAR(50),
     segment1_1 VARCHAR(50),
     segment2 VARCHAR(50),
     segment3 VARCHAR(50),
     segment4 VARCHAR(50),
     segment5 VARCHAR(50),
     segment6 VARCHAR(50),
     segment7 VARCHAR(50),
     segment8 VARCHAR(50),
     segment9 VARCHAR(50)
     )
     */

}
