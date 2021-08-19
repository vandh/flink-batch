package com.jw.plat.modules.ap;


import com.jw.plat.common.row.Rows;
import com.jw.plat.common.row.Tuples;
import com.jw.plat.common.select.Selector;
import com.jw.plat.common.select.Selectors2;
import com.jw.plat.modules.base.BaseExecute;
import com.jw.plat.modules.gl.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

/**
 * AP_XLA_DIS_LINKS：AE_LINE_NUM不同，所以开始会有重复记录
 * AP_XLA_AE_LINES: AE_LINE_NUM关联后，重复记录去掉了
 */
public class APGLExecute implements BaseExecute {

    @Override
    public DataSet<Row> run(ExecutionEnvironment env) throws RuntimeException {
        try {
           /* AP_GL_IMPORT_REFERENCES2.proc(env).print();*/
            DataSet<Row> input1 = null;
            input1 =GL_JE_BATCHES_POST.proc(env).join(GL_JE_HEADERS_POST3.proc(env))
                   .where(0)
                   .equalTo(8)
                   .with(new JoinFunction<Tuple7<String, String, String, String, String, String, String>,
                           Tuple11<String, String,String, String, String, String, String, String, String, String, String>,
                           Tuple10<String,String, String, String, String, String, String, String, String, String>>() {
                       @Override
                       public Tuple10<String, String,String, String, String, String, String, String, String, String> join(
                               Tuple7<String, String, String, String, String, String, String> t1,
                               Tuple11<String,String, String, String, String, String, String, String, String, String, String> t2) {
                           return Tuples.comb(t1, new int[]{0, 1, 4, 5}, t2, new int[]{0,1, 2, 3, 4, 8}, 10);
                       }
                   })
                   /**
                    * * je_batch_id
                    * * name
                    * * default_period_name
                    * * org_id
                    * * je_header_id
                    *   ledger_id
                    * * je_source
                    * * period_name
                    * * name
                    * * je_batch_id
                    */

                   .join(GL_JE_LINES_POST.proc(env))
                   .where(4, 7)
                   .equalTo(0, 4)
                   .with(new JoinFunction<Tuple10<String, String,String, String, String, String, String, String, String, String>,
                           Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                           Tuple12<String,String,String, String, String, String, String, String, String, String, String, String>>() {
                       @Override
                       public Tuple12<String,String, String, String, String, String, String, String, String, String, String,String> join(
                               Tuple10<String,String, String, String, String, String, String, String, String, String> t1,
                               Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> t2) throws Exception {
                           t2.f1 = t2.f1.replaceAll(",","");
                           return Tuples.comb(t1, t2, new int[]{1,3}, 12);
                       }
                   })

        /**
                 * je_batch_id
                 * name
                 * default_period_name
                 * org_id
                 * je_header_id
                 * ledger_id
                 * je_source
                 * period_name
                 * name
                 * je_batch_id
                 * je_line_num
                * CODE_COMBINATION_ID
                 */
                .join(QGL_SEQUENCE_VALUE.proc(env))
                .where(4)
                .equalTo(0)
                .with(new JoinFunction<Tuple12<String, String,String,String, String, String, String, String, String, String, String, String>,
                        Tuple4<String, String, String, String>,
                        Tuple13<String, String,String,String, String, String, String, String, String, String, String, String, String>>() {
                    @Override
                    public Tuple13<String, String,String,String, String, String, String, String, String, String, String, String, String> join(
                            Tuple12<String,String,String, String, String, String, String, String, String, String, String, String> t1,
                            Tuple4<String, String, String, String> t2) throws Exception {
                        return Tuples.comb(t1, t2, new int[]{1}, 13);
                    }
                })
//                /**
//                 * je_batch_id
//                 * name
//                 * default_period_name
//                 * org_id
//                 * je_header_id
//                  *ledger_id
//                 * je_source
//                 * period_name
//                 * name
//                 * je_batch_id
//                 * je_line_num
//                 * CODE_COMBINATION_ID
//                 * sequence_value
                    //reference_5
                    // chart_of_accounts_id
                    // summary_flag
//                 */
                .join(AP_GL_IMPORT_REFERENCES2.proc(env))
                .where(4, 10)
                .equalTo(0, 1)
                .with(new JoinFunction<Tuple13<String, String,String,String, String, String, String, String, String, String, String, String, String>,
                        Tuple3<String, String, String>,
                        Row>() {
                    @Override
                    public Row join(
                            Tuple13<String, String,String,String, String, String, String, String, String, String, String, String, String> t1,
                            Tuple3<String, String, String> t2) throws Exception {
                        return Rows.create(t1, t2, new int[]{2});
                    }
                })
                    .join(GL_CODE_COMBINATIONS2.proc(env))
                    .where(new Selector(11))
                    .equalTo(0)
                    .with(new JoinFunction<Row,Tuple12<String,String,String,String,String,String,String,String,String,String,String,String>,Row>() {
                        @Override
                        public Row join(Row t1,
                        Tuple12<String,String,String,String,String,String,String,String,String,String,String,String> t2) throws Exception {
                            return Rows.create(t1, t2, new int[]{1,2});
                        }
                    })
                    .join(GL_LEDGERS.proc(env))
                    .where(new Selectors2(new int[]{5,11}))
                    .equalTo(0,1)
                    .with(new JoinFunction<Row,Tuple2<String,String>,Row>() {
                        @Override
                        public Row join(Row t1,Tuple2<String,String> t2) throws Exception {
                            return Rows.create(t1, t2, new int[]{1,2});
                        }
                    })

            ;
        return input1;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
