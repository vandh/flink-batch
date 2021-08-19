package com.jw.plat.modules.gl;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.select.Selector;
import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

import java.io.File;

/**
 * 参数：
 * 文件位置
 * 批次
 * 账期
 * 字符集
 */
public class GLExecute7 implements BaseExecute {

    public DataSet<Row> run(ExecutionEnvironment env) throws RuntimeException {

        DataSet<Row> input1 = null;
        CsvReader csvReader = env.readCsvFile(Constants.OUTPATH + File.separator + Constants.GL2+"-"+Constants.batch);
//        csvReader.setCharset(Constants.GLCHARSET);
        input1 = csvReader.includeFields("111111111111111111111111")
                        .lineDelimiter("\n")
                        .fieldDelimiter(",")
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,
                                String.class,String.class,String.class,String.class,String.class,String.class,
                                String.class,String.class,String.class,String.class,String.class,String.class,
                                String.class,String.class,String.class,String.class,String.class,String.class)
                .joinWithHuge(GL_CODE_COMBINATIONS.proc(env))
                .where(19)
                .equalTo(0)
                .with(new JoinFunction<Tuple24<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                        Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Tuple24<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                    Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> t2) throws Exception {
                        return Rows.create(t1,t2);
                    }
                });

        return input1;

    }

    /**
     0- je_batch_id
     name
     * creation_date
     * created_by
     * default_period_name
     * org_id
     6- group_id

     7- je_header_id
     8- je_category
     9- je_source
     * period_name x
     10- name
     * currency_code
     12- actual_flag
     * je_batch_id  x
     14- currency_conversion_rate

     15- user_id
     16- user_name
     17 last_update_date

     18- je_category_name
     * user_je_category_name
     20- je_category_key

     je_header_id x
     21- sequence_value
     22- last_update_date
     23- ods_creation_date

     je_header_id  x
     24- post_person
     25- post_date
     * je_header_id x
     26- je_line_num
     27- ledger_id
     28- code_combination_id
     * period_name x
     29- effective_date
     * creation_date
     * created_by
     * accounted_dr
     * accounted_cr
     * description
     * gl_sl_link_id
     36- gl_sl_link_table
     37- reference_9
     * code_combination_id  x
     38- chart_of_accounts_id
     * summary_flag
     * segment1
     * segment2
     * segment3
     * segment4
     * segment5
     * segment6
     * segment7
     * segment8
     48- segment9
     */

}
