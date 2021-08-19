package com.jw.plat.modules.ra;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.row.Tuples;
import com.jw.plat.common.select.Selector;
import com.jw.plat.common.select.Selectors3;
import com.jw.plat.common.select.Selectors4;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

public class RAExecute  implements BaseExecute {


    @Override
    public DataSet<Row> run(ExecutionEnvironment env) {
        /**
         * RA_CUSTOMER_TRX_ALL {
         *
         *     /**
         *      * 	(39,11):          Rcta.Customer_Trx_Id                          1
         *                            Rcta.CREATED_BY                               5
         *      * 	(4,11):           Rcta.Trx_Number 业务系统单据编号,              7
         *      * 	(44,27):          Rcta.Set_Of_Books_Id                          10
         *      * 	(42,36):          Rcta.Bill_To_Customer_Id                      18
         *      * (3,11):             Rcta.Org_Id,                                  116
         *      */

          //   RA_CUST_TRX_LINE_GL_DIST {
            /**
             (57,46):          Rctlgda.Cust_Trx_Line_Gl_Dist_Id             1
             (41,11):          Rctla.Customer_Trx_Line_Id(+)                2
             (5,11):           Rctlgda.gl_posted_date 会计期间,              13
             (39,34):          Rctlgda.Customer_Trx_Id                      37



             (42,11):           Hca.Cust_Account_Id(+)                  1
             (43,28):           Hca.Party_Id                            2
             (11,11):           Hca.Account_Number 客户账户,             4



             (45,11):        Xte.Entity_Code                1
             (47,11):        Xte.Application_Id             2
             (48,11):        Xte.Entity_Id                  4
             (46,15):        Xte.Source_Id_Int_1            10
             (44,11):        Xte.Ledger_Id                  25


             (41,42):           Rctla.Customer_Trx_Line_Id(+)         1
             (40,37):           Rctla.Customer_Trx_Id(+)              7
             (26,11):           Rctla.description 摘要                12\


             (53,32):       Xdl.Application_Id                            1
             (54,30):       Xdl.Ae_Header_Id                              3
             (55,29):       Xdl.Ae_Line_Num                               4
             (56,11):       Xdl.Source_Distribution_Type                  5
             (57,11):       Xdl.Source_Distribution_Id_Num_1              6


             (43,11):           Hp.Party_Id(+)                  1
             (9,11):            Hp.party_number 客户编码,       2
             (10,11):           Hp.party_name   客户名称,       3

             (47,32):        Xe.Application_Id              2
             (48,27):        Xe.Entity_Id                   5


             (52,30):           Xal.Ae_Header_Id                            1
             (55,11):           Xal.Ae_Line_Num                             2
             (51,32):           Xal.Application_Id                          3
             (58,11):           Xal.CODE_COMBINATION_ID                     4
             (23,11):           Xal.ACCOUNTED_DR 借项,                       13
             (24,11):           Xal.ACCOUNTED_CR 贷项,                        14
             (12,11):           Xal.USSGL_TRANSACTION_CODE 凭证编号,         21


             (7,11):           Xah.Ae_Header_Id Je_Header_Id,       1
             (49,31):          Xah.Application_Id                   2
             (50,25):          Xah.Event_Id                         5
             (6,11):           Xah.Ae_batch_id（upg_batch_id） Je_batch_id,         66


             (58,35):      GCC.CODE_COMBINATION_ID          1         1
             gcc.segment1,                                  10
             gcc.segment2,                                  11
             gcc.segment3,                                  12
             gcc.segment4,                                  13
             gcc.segment5,                                  14
             gcc.segment6,                                  15
             gcc.segment7,                                  16
             gcc.segment8,                                  17
             gcc.segment9,                                  18


             (59,27):     FU.USER_ID              1
             fu.USER_NAME 制单人,      2

         */

        DataSet<Row> ds = null;

//       try {
         ds=  RA_CUSTOMER_TRX_ALL.proc(env).join(RA_CUST_TRX_LINE_GL_DIST.proc(env))
                    .where(0)
                    .equalTo(3)
                    .with(new JoinFunction<Tuple6<String, String, String, String, String, String>,
                            Tuple4<String, String, String, String>,
                            Tuple10<String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple10<String, String, String, String, String, String, String, String, String, String> join(
                                Tuple6<String, String, String, String, String, String> t1,
                                Tuple4<String, String, String, String> t2) throws Exception {
                            return Tuples.comb(t1, t2, 10);
                        }
                    })
                    /**
                     * select  Rcta.Customer_Trx_Id, Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id,Rcta.Org_Id,
                     * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id
                     * from RA_CUSTOMER_TRX_ALL Rcta   (join RA_CUST_TRX_LINE_GL_DIST Rctlgda   where  Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id)
                     */

                    .leftOuterJoin(Hz_Cust_Accounts.proc(env))
                    .where(4)
                    .equalTo(0)
                    .with(new JoinFunction<Tuple10<String, String, String, String, String, String, String, String, String, String>,
                            Tuple3<String, String, String>, Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple10<String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple3<String, String, String> t2) throws Exception {
                            if (t1 == null) t1 = Tuple10.of("", "", "", "", "", "", "", "", "", "");
                            if (t2 == null) t2 = Tuple3.of("", "", "");
                            return Tuples.comb(t1, t2, new int[]{1, 2}, 12);
                        }
                    })
//
//            //select  Rcta.Customer_Trx_Id, Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id,Rcta.Org_Id,
//        // Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
//        // Hca.Party_Id, Hca.Account_Number
//        // from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )
//todo join改成left
                    .join(AP_XLA_TRX_ENTITIES.proc(env))
                    .where(0, 3)
                    .equalTo(3, 4)
                    .with(new JoinFunction<Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>,
                            Tuple5<String, String, String, String, String>, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple5<String, String, String, String, String> t2) throws Exception {
                            return Tuples.comb(t1, t2, new int[]{0, 1}, 14);
                        }
                    })
//
////
////            /**
////             *  select  Rcta.Customer_Trx_Id, Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
////             *             // Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,9
////             *             // Hca.Party_Id, Hca.Account_Number,11
////             *             // Xte.Entity_Id, Xte.Application_Id, 13
////             *             // from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(join  Xla_Transaction_Entities on  Xte.Ledger_Id = Rcta.Set_Of_Books_Id and Nvl(Xte.Source_Id_Int_1, -99) = Rcta.Customer_Trx_Id )
////             */
////
                    .leftOuterJoin(RA_CUSTOMER_TRX_LINES_ALL.proc(env))
                    .where(7, 9)
                    .equalTo(0, 1)
                    .with(new JoinFunction<Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                            Tuple3<String, String, String>, Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple3<String, String, String> t2) throws Exception {
                            if (t1 == null) t1 = Tuple14.of("", "", "", "", "", "", "", "", "", "", "", "", "", "");
                            if (t2 == null) t2 = Tuple3.of("", "", "");
                            return Tuples.comb(t1, t2, new int[]{2}, 15);
                        }
                    })
///////**
////// * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
//// Rctlgda.Cust_Trx_Line_Gl_Dist_Id, Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
////// * Hca.Party_Id, Hca.Account_Number
////// *  Xte.Entity_Id, Xte.Application_Id, 13
////// * Rctla.description,14
////// * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))
////// */
////                 //todo 应该是join，现在为了 测试数据 给成 leftOuterJoin
                    .join(AP_XLA_DIS_LINKS.proc(env))
                    .where(6)
                    .equalTo(4)
                    .with(new JoinFunction<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                            Tuple5<String, String, String, String, String>, Tuple18<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple18<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple5<String, String, String, String, String> t2) throws Exception {
                            return Tuples.comb(t1, t2, new int[]{0, 1, 2}, 18);
                        }
                    })
//////            /**
//////             * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
//////             * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
//////             * Hca.Party_Id, Hca.Account_Number
//////             *  Xte.Entity_Id, Xte.Application_Id,
//////             * Rctla.description
//////             * Xdl.Application_Id,Xdl.Ae_Header_Id,Xdl.Ae_Line_Num
//////             * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）
//////              */
//
                    .leftOuterJoin(Hz_Parties.proc(env))
                    .where(10)
                    .equalTo(0)
                    .with(new JoinFunction<Tuple18<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                            Tuple3<String, String, String>, Tuple20<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple20<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple18<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple3<String, String, String> t2) throws Exception {
                            if (t1 == null)
                                t1 = Tuple18.of("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "");
                            if (t2 == null) t2 = Tuple3.of("", "", "");
                            return Tuples.comb(t1, t2, new int[]{1, 2}, 20);
                        }
                    })
////// * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
////// * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id
////// * Hca.Party_Id, Hca.Account_Number
////// *  Xte.Entity_Id, Xte.Application_Id,13
////// * Rctla.description
////// * Xdl.Application_Id，Xdl.Ae_Header_Id，Xdl.Ae_Line_Num
////// * Hp.party_number, Hp.party_name
////// * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）(right Join Hz_Parties on Hp.Party_Id(+) = Hca.Party_Id )
////// */
////                 //todo 应该是join 现在改成 leftOUTJoin
                    .join(Xla_Events.proc(env))
                    .where(13, 12)
                    .equalTo(0, 1)
                    .with(new JoinFunction<Tuple20<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                            Tuple2<String, String>, Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple22<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                                Tuple20<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple2<String, String> t2) throws Exception {
                            return Tuples.comb(t1, t2, new int[]{1, 2}, 22);
                        }
                    })

//
///////**
///////**
////// * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
////// * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
////// * Hca.Party_Id, Hca.Account_Number,
////// *  Xte.Entity_Id, Xte.Application_Id,13
////// * Rctla.description,14
////// * Xdl.Application_Id,Xdl.Ae_Header_Id,Xdl.Ae_Line_Num,17
////// * Hp.party_number, Hp.party_name
////// * Xe.Application_Id ,Xe.Event_Id
////// * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）(right Join Hz_Parties on Hp.Party_Id(+) = Hca.Party_Id )
////// */
////
                    //todo join 改成leftjoin
                    .join(Xla_Ae_Lines.proc(env))
                    .where(16,17,15)
                    .equalTo(0,1,2)
                    .with(new JoinFunction<Tuple22<String,String,String, String,  String,  String,  String,String, String, String, String, String, String, String, String, String,String,  String, String, String, String, String>,
                            Tuple7<String, String, String,String, String, String, String>, Row>() {
                        @Override
                        public Row join(
                                Tuple22<String, String,String,String,  String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple7<String,String,String,String,String,String,String > t2) throws Exception {
                            if(t1==null) t1= Tuple22.of("","","","","","","","","","","","","","","","","","","","","","");
                            if(t2==null) t2= Tuple7.of("","","","","","","");
                            return Rows.create(t1,t2, new int[]{0,1,2,3,4,5,6});
                        }
                    })
//////            /**
//////             * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
//////             * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
//////             * Hca.Party_Id, Hca.Account_Number,
//////             * Xte.Entity_Id, Xte.Application_Id, 13
//////             * Rctla.description,14
//////             * Xdl.Application_Id,Xdl.Ae_Header_Id,Xdl.Ae_Line_Num,17
//////             * Hp.party_number, Hp.party_name19
//////             * Xe.Application_Id ,Xe.Event_Id,21
//////             * Xal.Ae_Header_Id, Xal.Ae_Line_Num, Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号28
//////             * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）(right Join Hz_Parties on Hp.Party_Id(+) = Hca.Party_Id )
//////             */
//////
////
                    //todo jonin 改成 leftjoin
                .join(Xla_Ae_Headers.proc(env))
                .where(new Selectors4(new int[]{22,20,21,24}))
                .equalTo(0,1,2,1)
                .with(new JoinFunction<Row, Tuple4< String,String,String,String >, Row>() {
                    @Override
                    public Row join(Row t1, Tuple4<String, String, String, String> t2) throws Exception {
                        //todo 此处会报错
                        if(t2==null) t2= Tuple4.of("","","","");
                        return Rows.create(t1,t2, new int[]{0,3});
                    }
                })
//////            /**
//////             * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
//////             * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
//////             * Hca.Party_Id, Hca.Account_Number,
//////             * Xte.Entity_Id, Xte.Application_Id,
//////             * Rctla.description,14
//////             * Xdl.Application_Id, Xdl.Ae_Header_Id, Xdl.Ae_Line_Num,
//////             * Hp.party_number, Hp.party_name,18
//////             * Xe.Application_Id ,Xe.Event_Id,
//////             * Xal.Ae_Header_Id, Xal.Ae_Line_Num, Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号
//////             * Xah.Ae_Header_Id Je_Header_Id, Xah.Ae_batch_id（upg_batch_id）
//////             * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）(right Join Hz_Parties on Hp.Party_Id(+) = Hca.Party_Id )
//////             * gcc.segment1, gcc.segment2, gcc.segment3, gcc.segment4,gcc.segment5,gcc.segment6,  gcc.segment7,gcc.segment8,  gcc.segment9,
//////             */
////                 //todo 本来是Join  现在改成leftjoin
                .join(GL_CODE_COMBINATIONS.proc(env))
                .where(new Selector(25))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple10< String,String,String,String,String, String,String,String,String,String>, Row>() {
                    @Override
                    public Row join(Row t1, Tuple10< String,String,String,String,String, String,String,String,String,String > t2) throws Exception {
                        if(t2==null) t2= Tuple10.of("","","","","","","","","","");
                        return Rows.create(t1,t2, new int[]{1,2,3,4,5,6,7,8,9});
                    }
                })
///////**
////// * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
////// * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,
////// * Hca.Party_Id, Hca.Account_Number,
////// *  Xte.Application_Id, Xte.Entity_Id,
////// * Rctla.description,
////// * Xdl.Application_Id,Xdl.Ae_Header_Id,Xdl.Ae_Line_Num,
////// * Hp.party_number, Hp.party_name
////// * Xe.Application_Id ,Xe.Event_Id,
////// * Xal.Ae_Header_Id, Xal.Ae_Line_Num, Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号
////// * Xah.Ae_Header_Id Je_Header_Id, Xah.Ae_batch_id（upg_batch_id）
////// * gcc.segment1,  gcc.segment2, gcc.segment3, gcc.segment4,gcc.segment5,gcc.segment6,gcc.segment7, gcc.segment8, gcc.segment9,
////// * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）(right Join Hz_Parties on Hp.Party_Id(+) = Hca.Party_Id )
////// *
////// */
        //todo join 改成leftjoin
                .join(FND_USER.proc(env))
                .where(new Selector(1))
                .equalTo(0)
                .with(new JoinFunction<Row, Tuple2< String,String>, Row>() {
                    @Override
                    public Row join(Row t1, Tuple2< String,String> t2) throws Exception {
                        if(t2==null) t2= Tuple2.of("","");
                        return Rows.create(t1,t2, new int[]{1});
                    }
                });
//                    .print();
//       }catch (Exception E){
//           E.printStackTrace();
//       };

///**
// * select  Rcta.Customer_Trx_Id,  Rcta.CREATED_BY, Rcta.Trx_Number, Rcta.Set_Of_Books_Id, Rcta.Bill_To_Customer_Id, Rcta.Org_Id,
// *  * Rctlgda.Cust_Trx_Line_Gl_Dist_Id,  Rctlgda.Customer_Trx_Line_Id, Rctlgda.gl_posted_dat, Rctlgda.Customer_Trx_Id,9
// *  * Hca.Party_Id, Hca.Account_Number,11
// *  * Xte.Entity_Id, Xte.Application_Id,13
// *  * Rctla.description,14
// *  * Xdl.Application_Id, Xdl.Ae_Header_Id, Xdl.Ae_Line_Num,17
// *  * Hp.party_number, Hp.party_name,19
// *  * Xe.Application_Id ,Xe.Event_Id,21
// *  * Xal.Ae_Header_Id, Xal.Ae_Line_Num, Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号 28
// *  * Xah.Ae_Header_Id Je_Header_Id, Xah.Ae_batch_id（upg_batch_id）30
// *  *gcc.segment1,  gcc.segment2, gcc.segment3, gcc.segment4,gcc.segment5,gcc.segment6,gcc.segment7, gcc.segment8, gcc.segment9,
// *   fu.USER_NAME 制单人,
// *  * from RA_CUSTOMER_TRX_ALL Rcta  ( join RA_CUST_TRX_LINE_GL_DIST Rctlgda on Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id  )  (right join  Hz_Cust_Accounts on  Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id )(left join Ra_Customer_Trx_Lines_All on Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+) And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+))（join Xla_Distribution_Links on Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id）(right Join Hz_Parties on Hp.Party_Id(+) = Hca.Party_Id )
// *  *
// */
       return ds;
    }
}
/**
 * -- 应收发票：
 *
 *     Select Rcta.Org_Id,
 *            Rcta.Trx_Number 业务系统单据编号,
 *            Rctlgda.gl_posted_date 会计期间,
 *            Xah.Ae_batch_id Je_batch_id,
 *            Xah.Ae_Header_Id Je_Header_Id,
 *            Xal.Ae_Line_Num Je_Line_Num,
 *            Hp.party_number 客户编码,
 *            Hp.party_name   客户名称,
 *            Hca.Account_Number 客户账户,
 *            Xal.USSGL_TRANSACTION_CODE 凭证编号,
 *            CONCAT(gcc.segment1,'.',gcc.segment2,'.',gcc.segment3,'.',gcc.segment4,'.',gcc.segment5,'.',gcc.segment6,'.',gcc.segment7,'.',gcc.segment8,'.',gcc.segment9) 科目组合,
 *            gcc.segment1,
 *            gcc.segment2,
 *            gcc.segment3,
 *            gcc.segment4,
 *            gcc.segment5,
 *            gcc.segment6,
 *            gcc.segment7,
 *            gcc.segment8,
 *            gcc.segment9,
 *            Xal.ACCOUNTED_DR 借项,
 *            Xal.ACCOUNTED_CR 贷项,
 *            fu.USER_NAME 制单人,
 *            Rctla.description 摘要
 *       From Ra_Customer_Trx_All          Rcta,
 *            Ra_Cust_Trx_Line_Gl_Dist_All Rctlgda,
 *
 *
 *
 *            Ra_Customer_Trx_Lines_All    Rctla,
 *            Hz_Cust_Accounts             Hca,
 *            Hz_Parties                   Hp,
 *            Xla_Transaction_Entities Xte,
 *            Xla_Events                   Xe,
 *            Xla_Ae_Headers               Xah,
 *            Xla_Ae_Lines                 Xal,
 *            Xla_Distribution_Links       Xdl,
 *            gl_code_combinations_01          GCC,
 *            FND_USER_01                      FU
 *      Where Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id
 *
 *        And Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+)
 *        And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+)
 *        And Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id
 *        And Hp.Party_Id(+) = Hca.Party_Id
 *        And Xte.Ledger_Id = Rcta.Set_Of_Books_Id
 *        And Xte.Entity_Code = 'TRANSACTIONS'
 *        And Nvl(Xte.Source_Id_Int_1, -99) = Rcta.Customer_Trx_Id
 *        And Xte.Application_Id = Xe.Application_Id
 *        And Xte.Entity_Id = Xe.Entity_Id
 *        And Xe.Application_Id = Xah.Application_Id
 *        And Xe.Event_Id = Xah.Event_Id
 *        And Xah.Application_Id = Xal.Application_Id
 *        And Xah.Ae_Header_Id = Xal.Ae_Header_Id
 *        And Xal.Application_Id = Xdl.Application_Id
 *        And Xal.Ae_Header_Id = Xdl.Ae_Header_Id
 *        And Xal.Ae_Line_Num = Xdl.Ae_Line_Num
 *        And Xdl.Source_Distribution_Type = 'RA_CUST_TRX_LINE_GL_DIST_ALL'
 *        And Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id
 *        and Xal.CODE_COMBINATION_ID=GCC.CODE_COMBINATION_ID
 *        AND AI.CREATED_BY = FU.USER_ID
 *
 */


