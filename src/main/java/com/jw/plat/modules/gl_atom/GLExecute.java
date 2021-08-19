package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.select.Selector;
import com.jw.plat.common.util.Constants;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

/**
 * 参数：
 * 文件位置
 * 批次
 * 账期
 * 字符集
 */
public class GLExecute implements BaseExecute {
    /**
     *  1：GL_JE_BATCHES
     *  2：Gl_JE_HEADERS
     *  3：Gl_JE_LINES
     *  4：GL_CODE_COMBINATIONS
     *  5：QGL_SEQUENCE_VALUE
     *  6：Gl_Je_Categories
     *  7：qgl_approve
     *  8：FND_USER_01
     *
     *  ------------
     *   GL_JE_BATCHES_POST 0 * 0:je_batch_id  1
     * 					    1 * 1:name  5
     * 					    2 * 2:creation_date  13
     * 					    3 * 3:created_by  14
     * 					    4 * 4:default_period_name  17
     * 					    5 * 5:org_id   46
     * 					    6 * 6:group_id  51
     * 	GL_JE_HEADERS_POST  7 * 0:je_header_id     1 0
     * 					    8 * 1:je_category  5
     * 					    9 * 2:je_source    6
     * 					    10 * 3:period_name  7
     * 					    11 * 4:name 8
     * 					    12 * 5:currency_code    9
     * 					    13 * 6:actual_flag      14
     * 					    14 * 7:je_batch_id      25
     * 					    15 * 8:currency_conversion_rate     41
     * 	GL_JE_LINES_POST    16 * 0：je_header_id
     * 					    17* 1：je_line_num
     * 					    18 * 2：ledger_id
     * 					    19 * 3：code_combination_id
     * 					    20 * 4：period_name
     * 					    21 * 5：effective_date
     * 					    22 * 6：creation_date
     * 					    23 * 7：created_by
     * 					    24 * 8：accounted_dr
     * 					    25 * 9：accounted_cr
     * 					    26 * 10：description
     * 					    27 * 11：gl_sl_link_id
     * 					    28 * 12：gl_sl_link_table
     * 					    29 * 13：reference_9
     * QGL_SEQUENCE_VALUE   30*0: je_header_id
     * 					    31*1: sequence_value
     * 					    32 *2: last_update_date
     * 					    33 *3: ods_creation_date
     * GL_JE_CATEGORIES_TL  34 * 0：je_category_name 1
     * 					    35 * 1：language
     * 					    36 * 2：user_je_category_name 4
     * GL_JE_SOURCES_TL     37 *0: je_source_name 0
     *                      38 *1: language 1
     *                      39 *2: user_je_source_name 6
     * FND_USER_01          40 * 0：user_id
     * 					    41 * 1：user_name
     * 					    42 * 2：last_update_date
     * QGL_APPROVE          43 * 0:je_header_id
     * 						44 * 1:post_person
     * 						45 * 2:post_date
     * GL_CODE_COMBINATIONS	46 * 0：code_combination_id
     * 					    47 * 1：chart_of_accounts_id
     * 					    48 * 2：summary_flag
     * 					    49 * 3：segment1
     * 					    50 * 4：segment2
     * 					    51 * 5：segment3
     * 					    52 * 6：segment4
     * 					    53 * 7：segment5
     * 					    54 * 8：segment6
     * 					    55 * 9：segment7
     * 					    56 * 10：segment8
     * 					    57 * 11：segment9
     */

    public DataSet<Row> run(ExecutionEnvironment env) throws RuntimeException {

        DataSet<Row> input1 = null;
        input1 = GL_JE_BATCHES_POST.proc(env).join(GL_JE_HEADERS_POST.proc(env))
                .where(0)
                .equalTo(7)
                .with(new JoinFunction<Tuple5<String, String, String, String, String>,
                                       Tuple10<String,String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Tuple5< String, String, String, String, String> T_GL_JE_BATCHES_POST,
                                    Tuple10<String,String, String, String, String, String, String, String, String, String> T_GL_JE_HEADERS_POST) throws Exception {
                        T_GL_JE_BATCHES_POST.f1 = T_GL_JE_BATCHES_POST.f1 == null ? "" : T_GL_JE_BATCHES_POST.f1.replaceAll(Constants.A10, "").replaceAll(Constants.A13, "").replaceAll(Constants.A34, "").replaceAll(Constants.A39, "");
                        return Rows.create(T_GL_JE_BATCHES_POST, T_GL_JE_HEADERS_POST);
                        /**
                         *  *   GL_JE_BATCHES_POST  0 * 0:je_batch_id                   1
                         *  * 					    1 * 1:name                          1
                         *  * 					    2 * 2:creation_date                 1
                         *  * 					    3 * 3:created_by                    1
                         *  * 					    4 * 4:default_period_name           1
                         *  * 	GL_JE_HEADERS_POST  5 * 0:je_header_id                  1
                         *  * 					    6 * 1:je_category                   1
                         *  * 					    7 * 2:je_source                     1
                         *  * 					    8 * 3:period_name                  0
                         *  * 					    9 * 4:name                         1
                         *  * 					    10 * 5:currency_code                1
                         *  * 					    11 * 6:actual_flag                  0
                         *  * 					    12 * 7:je_batch_id                  0
                         *                         13 * description
                         *  * 					    14 * 8:currency_conversion_rate     1
                         */
                    }
                })
                .join(GL_JE_LINES_POST.proc(env))
                .where(new Selector(5))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>  T_GL_JE_LINES_POST) throws Exception {
                        return Rows.create(gl_row, T_GL_JE_LINES_POST);
                        /**
                            GL_JE_LINES_POST    15 * 0：je_header_id                0
                         * 					    16*  1：je_line_num                 1
                         * 					    17 * 2：ledger_id                   0
                         * 					    18 * 3：code_combination_id         1
                         * 					    19 * 4：period_name                 0
                         * 					    20 * 5：effective_date              0
                         * 					    21 * 6：creation_date               0
                         * 					    14 * 7：created_by                  0
                         * 					    15 * 8：accounted_dr                1
                         * 					    16 * 9：accounted_cr                1
                         * 					    17 * 10：description                1
                         * 					    17 * 11：gl_sl_link_id              0
                         * 					    17 * 12：gl_sl_link_table           0
                         * 					    18 * 13：reference_9                1
                         */
                    }
                })
                .join(GL_JE_CATEGORIES_TL.proc(env))
                .where(new Selector(6))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple3<String, String, String> T_GL_JE_CATEGORIES_TL) throws Exception {
                        return Rows.create(gl_row, T_GL_JE_CATEGORIES_TL);
                        /**
                         *GL_JE_CATEGORIES_TL   18 * 0：je_category_name          0
                         *					    18 * 1：language                  0
                         *					    19 * 2：user_je_category_name     1
                         */
                    }
                })
                .join(GL_JE_SOURCES_TL.proc(env))
                .where(new Selector(7))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple3<String, String, String> GL_JE_SOURCES_TL) throws Exception {
                        return Rows.create(gl_row, GL_JE_SOURCES_TL);
                        /**
                         * *GL_JE_SOURCES_TL       19 *0: je_source_name             0
                         *  *                      19 *1: language                   0
                         *  *                      20 *2: user_je_source_name        1
                         */
                    }
                })
                .join(QGL_SEQUENCE_VALUE.proc(env))
                .where(new Selector(5))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple4<String, String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple4<String, String, String, String> T_QGL_SEQUENCE_VALUE) throws Exception {
                        T_QGL_SEQUENCE_VALUE.f1 = T_QGL_SEQUENCE_VALUE.f1 == null ? "" : T_QGL_SEQUENCE_VALUE.f1.replaceAll(",", "&");
                        return Rows.create(gl_row, T_QGL_SEQUENCE_VALUE);
                        /**
                         *  * QGL_SEQUENCE_VALUE    20 *0: je_header_id                 0
                         *  * 					    21 *1: sequence_value               1
                         *  * 					    21 *2: last_update_date            0
                         *  * 					    21 *3: ods_creation_date           0
                         */
                    }
                })
                .join(GL_CODE_COMBINATIONS.proc(env))
                .where(new Selector(18))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> T_GL_CODE_COMBINATIONS) throws Exception {
                        return Rows.create(gl_row, T_GL_CODE_COMBINATIONS);
                        /**
                         *  * GL_CODE_COMBINATIONS	23 * 0：code_combination_id         0
                         *  * 					    23 * 1：chart_of_accounts_id       0
                         *  * 					    23 * 2：summary_flag              0
                         *  * 					    24 * 3：segment1                 1
                         *  * 					    25 * 4：segment2                  1
                         *  * 					    26 * 5：segment3                 1
                         *  * 					    27 * 6：segment4                 1
                         *  * 					    28 * 7：segment5                 1
                         *  * 					    29 * 8：segment6                 1
                         *  * 					    30 * 9：segment7                 1
                         *  * 					    31 * 10：segment8                 1
                         *  * 					    32 * 11：segment9                 1
                         */
                    }
                })
                .leftOuterJoin(FND_USER.proc(env))
                .where(new Selector(3))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple3<String, String, String> T_FND_USER) throws Exception {
                        if(T_FND_USER==null) return Rows.create(gl_row, Tuple3.of("","",""));
                        else
                        return Rows.create(gl_row, T_FND_USER);
                        /**
                         *  *  FND_USER_01          21 * 0：user_id                    0
                         *  * 					    22 * 1：user_name                  1
                         *  * 					    22 * 2：last_update_date           0
                         */
                    }
                })
                .leftOuterJoin(QGL_APPROVE.proc(env))
                .where(new Selector(5))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple3<String, String, String>, Row>() {
                    @Override
                    public Row join(Row gl_row, Tuple3<String, String, String> T_QGL_APPROVE) throws Exception {
                        if(T_QGL_APPROVE==null) return Rows.create(gl_row, Tuple2.of("",""));
                        else
                        return Rows.create(gl_row, T_QGL_APPROVE);
                        /**
                         *  * QGL_APPROVE           22 * 0:je_header_id               0
                         *  * 						22 * 1:post_person                0
                         *  * 						23 * 2:post_date                  1
                         */
                    }
                })
                ;
        return input1;
    }


}
