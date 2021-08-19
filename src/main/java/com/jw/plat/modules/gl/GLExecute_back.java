package com.jw.plat.modules.gl;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.select.Selector;
import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

/**
 * 参数：
 * 文件位置
 * 批次
 * 账期
 * 字符集
 */
public class GLExecute_back implements BaseExecute {

    public DataSet<Row> run(ExecutionEnvironment env) throws RuntimeException {

        DataSet<Row> input1 = null;
        input1 = GL_JE_BATCHES_POST.proc(env).join(GL_JE_HEADERS_POST.proc(env))
                .where(0)
                .equalTo(7)
                .with(new JoinFunction<Tuple7<String, String, String, String, String, String, String>, Tuple9<String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Tuple7<String, String, String, String, String, String, String> t7,
                                    Tuple9<String, String, String, String, String, String, String, String, String> t9) throws Exception {
                        t7.f1 = t7.f1 == null ? "" : t7.f1.replaceAll(Constants.A10, "").replaceAll(Constants.A13, "").replaceAll(Constants.A34, "").replaceAll(Constants.A39, "");
                        return Rows.create(t7, t9);
                    }
                })
                .leftOuterJoin(FND_USER.proc(env))
                .where(new Selector(3))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row r14, Tuple3<String, String, String> t3) throws Exception {
                        return Rows.create(r14, t3);
                    }
                })
                .join(GL_JE_CATEGORIES_TL.proc(env))
                .where(new Selector(8))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row r17, Tuple3<String, String, String> t3) throws Exception {
                        return Rows.create(r17, t3);
                    }
                })
                .join(QGL_SEQUENCE_VALUE.proc(env))
                .where(new Selector(7))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple4<String, String, String, String>, Row>() {
                    @Override
                    public Row join(Row r20, Tuple4<String, String, String, String> t4) throws Exception {
                        return Rows.create(r20, t4, new int[]{1, 2, 3});
                    }
                })
                .leftOuterJoin(QGL_APPROVE.proc(env))
                .where(new Selector(7))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row r23, Tuple3<String, String, String> t3) throws Exception {
                        return Rows.create(r23, t3, new int[]{1, 2});
                    }
                })
                .join(GL_JE_LINES_POST.proc(env))
                .where(new Selector(7))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Row r25, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> t14) throws Exception {
                        return Rows.create(r25, t14, new int[]{1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13});
                    }
                })
                .join(GL_CODE_COMBINATIONS.proc(env))
                .where(new Selector(28))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Row r37, Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> t12) throws Exception {
                        return Rows.create(r37, t12, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
                    }
                });


        try {
//            input1.print();
//            GL_JE_BATCHES_POST.proc(env).print();
//          GL_JE_HEADERS_POST.proc(env).print();
//            CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_HEADERS_POST, Constants.batch));
//            csvReader.setCharset(Constants.GLCHARSET);
//                    csvReader.includeFields("1")
//                            .lineDelimiter(Constants.LF)
//                            .fieldDelimiter(Constants.LF)
//                            .ignoreInvalidLines()
//                            .types(String.class).map(
//                            new MapFunction<Tuple1<String>, Tuple1<String>>() {
//                                long count=0;
//                                @Override
//                                public Tuple1<String> map(Tuple1<String> t) throws Exception {
//                                    if(t.f0.trim().indexOf("8866616")!=-1)
//                                        System.out.println(t.f0);
//                                    return Tuple1.of("");
//                                }
//                            }
//                    ).print();


//            GL_JE_BATCHES_POST.proc(env).print();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


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
