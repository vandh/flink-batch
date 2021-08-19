package com.jw.plat.modules.ar;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.row.Tuples;
import com.jw.plat.common.select.Selector;
import com.jw.plat.common.select.Selectors2;
import com.jw.plat.common.select.Selectors3;
import com.jw.plat.common.select.Selectors4;
import com.jw.plat.modules.base.BaseExecute;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.Row;

import static org.apache.commons.lang3.StringUtils.join;

public class ARExecute implements BaseExecute {

    /**
     * AR_CASH_RECEIPTS_ALL {}
     *      Acra.Cash_Receipt_Id                            1
     *      Acra.CREATED_BY                                  5
     *      Acra.Set_Of_Books_Id                             8
     *      Acra.Receipt_Number 业务系统单据编号,             14
     *      Acra.comments 备注                               17
     *       Acra.Org_Id,                                    64
     *
     *AR_CASH_RECEIPT_HISTORY_ALL
     *      * (41,11):            Acrha.Cash_Receipt_History_Id                  1
     *      * (40,34):            Acrha.Cash_Receipt_Id                          2
     *        * (6,11):             Acrha.gl_date 会计期间,                       9
     *
     *
     * Ar_Distributions_All
     *      * 	(58,46):       Ada.Line_Id                                      1
     *        (41,43):       Ada.Source_Id                                      2
     *      * 	(42,11):       Ada.Source_Table in ('CRH','RA','MCD')           3
     *      * 	(43,36):       Ada.Third_Party_Id                               27
     *
     *
     * AP_XLA_TRX_ENTITIES
     *  * (49,11):       And Xte.Entity_Id = Xe.Entity_Id                                        1
     *      * (48,11):       And Xte.Application_Id = Xe.Application_Id                          2
     *      * (46,11):       And Xte.Entity_Code = 'RECEIPTS'                                    4
     *      * (47,15):       And Nvl(Xte.Source_Id_Int_1, -99) = Acra.Cash_Receipt_Id            10
     *      * (45,11):       And Xte.Ledger_Id = Acra.Set_Of_Books_Id                            25
     *
     *
     * HZ_CUST_ACCOUNTS
     *      (43,11):           Hca.Cust_Account_Id(+)                      1
     *      (44,28):           Hp.Party_Id(+) = Hca.Party_Id               2
     *      (12,11):           Hca.Account_Number 客户账户,                4
     *
     *
     *     Hz_Parties
     *      (44,11):            And Hp.Party_Id(+) = Hca.Party_Id              1
     *      (10,11):           Hp.party_number 客户编码,                       2
     *      (11,11):           Hp.party_name   客户名称,                       3
     *
     *
     *  Xla_Events
     *      (47,32):        Xe.Application_Id              2
     *      (48,27):        Xe.Entity_Id                   5
     *
     *      Xla_Ae_Headers
     *                               Xah.Ae_Header_Id Je_Header_Id,                              1
     *      * 	(52,11):            And Xah.Application_Id = Xal.Application_Id                  2
     *      * 	(51,25):            And Xe.Event_Id = Xah.Event_Id                               5
     *      * 	(7,11):             Xah.Ae_batch_id（upg_batch_id） Je_batch_id,                  66
     *
     *
     *      Xla_Ae_Lines
     *      (52,30):           Xal.Ae_Header_Id                            1
     *      (55,11):           Xal.Ae_Line_Num                             2
     *      (51,32):           Xal.Application_Id                          3
     *      (58,11):           Xal.CODE_COMBINATION_ID                     4
     *      (23,11):           Xal.ACCOUNTED_DR 借项,                       13
     *      (24,11):           Xal.ACCOUNTED_CR 贷项,                        14
     *      (12,11):           Xal.USSGL_TRANSACTION_CODE 凭证编号,         21
     *
     *
     * AP_XLA_DIS_LINKS
     *  * 	(54,32):           And Xal.Application_Id = Xdl.Application_Id                  1
     *      * 	(55,30):       And Xal.Ae_Header_Id = Xdl.Ae_Header_Id                      3
     *      * 	(56,29):       And Xal.Ae_Line_Num = Xdl.Ae_Line_Num                        4
     *      * 	(57,11):       And Xdl.Source_Distribution_Type = 'AR_DISTRIBUTIONS_ALL'    5
     *      * 	(58,11):       And Xdl.Source_Distribution_Id_Num_1 = Ada.Line_Id           6
     *
     *
     *
     *      GL_CODE_COMBINATIONS
     *      (58,35):      GCC.CODE_COMBINATION_ID          1
     *      gcc.segment1,                                  10
     *      gcc.segment2,                                  11
     *      gcc.segment3,                                  12
     *      gcc.segment4,                                  13
     *      gcc.segment5,                                  14
     *      gcc.segment6,                                  15
     *      gcc.segment7,                                  16
     *      gcc.segment8,                                  17
     *      gcc.segment9,                                  18
     *
     *      FND_USER
     *                    FU.USER_ID              1
     *                  fu.USER_NAME 制单人,      2
     *
     */

    @Override
    public DataSet<Row> run(ExecutionEnvironment env) {
        DataSet<Row> ds =null;
        try{
                AR_CASH_RECEIPTS_ALL.proc(env).join(AR_CASH_RECEIPT_HISTORY_ALL.proc(env))
                .where(0)
                .equalTo(1)
                .with(new JoinFunction<Tuple6<String, String,String, String,String, String>,
                        Tuple3<String, String, String>,
                        Tuple9<String, String, String, String, String, String, String,  String, String>>() {
                    @Override
                    public  Tuple9<String, String, String, String, String, String, String,  String, String> join(
                            Tuple6<String, String, String, String, String,String > t1,
                            Tuple3<String, String, String> t2) throws Exception {
                        return Tuples.comb(t1,t2,9);
                    }
                })
        /**
         * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,5
         * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
         * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
         *
         */
            .join(Ar_Distributions_All.proc(env))
                .where(6)
                .equalTo(1)
                .with(new JoinFunction<Tuple9<String, String, String, String, String, String, String,  String, String>,
                        Tuple4<String, String, String, String>,
                        Tuple11<String, String, String, String, String, String, String,  String, String, String, String>>() {
                    @Override
                    public  Tuple11<String, String, String, String, String, String, String,  String, String, String, String> join(
                            Tuple9<String, String, String, String, String, String, String,  String, String > t1,
                            Tuple4<String, String, String, String> t2) throws Exception {
                        return Tuples.comb(t1,t2,  new int[]{0,3},11);
                    }
                })
//
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,
//                 * Ada.Line_Id, Ada.Third_Party_Id,
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
//
                        //todo   join 改成leftjoin
                .join(AP_XLA_TRX_ENTITIES.proc(env))
                        .where(2,0)
                        .equalTo(4,3)
                        .with(new JoinFunction<Tuple11<String, String, String, String, String, String, String,  String, String,  String, String>,
                                Tuple5<String, String, String, String, String>,
                                Tuple13<String, String, String, String, String, String, String,  String, String, String, String, String, String>>() {
                            @Override
                            public  Tuple13<String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                                    Tuple11<String, String, String, String, String, String, String,  String, String,  String, String> t1,
                                    Tuple5<String, String, String, String, String> t2) throws Exception {
                                return Tuples.comb(t1,t2, new int[]{0,1},13);
                            }
                        })
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments, Acra.Org_Id,5
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id 12
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
                .leftOuterJoin(HZ_CUST_ACCOUNTS.proc(env))
                .where(10)
                .equalTo(0)
                .with(new JoinFunction<Tuple13<String, String, String, String, String,String,String, String, String, String,  String, String,  String>,
                        Tuple3<String, String, String>,
                        Tuple15<String, String, String, String, String, String, String,  String, String, String, String, String, String, String, String>>() {
                    @Override
                    public  Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                            Tuple13<String, String, String, String, String,String,String, String, String, String,  String, String,  String> t1,
                            Tuple3<String, String, String> t2) throws Exception {
                        return Tuples.comb(t1,t2,  new int[]{1,2},15);
                    }
                })
//
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,
//                 * Ada.Line_Id, Ada.Third_Party_Id,
//                 * Xte.Entity_Id, Xte.Application_Id,12
//                 * Hca.Party_Id,  Hca.Account_Number,14
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
                .leftOuterJoin(Hz_Parties.proc(env))
                .where(13)
                .equalTo(0)
                .with(new JoinFunction<Tuple15<String, String, String, String, String, String, String,  String, String, String, String, String, String, String, String>,
                        Tuple3<String, String, String>,
                        Tuple17<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String>>() {
                    @Override
                    public  Tuple17<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String> join(
                            Tuple15<String, String, String, String, String, String, String,  String, String, String, String, String, String, String, String> t1,
                            Tuple3<String, String, String> t2) throws Exception {
                        return Tuples.comb(t1,t2,  new int[]{1,2},17);
                    }
                })
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id,
//                 * Hca.Party_Id,  Hca.Account_Number,
//                 * Hp.party_number 客户编码,  Hp.party_name 16  客户名称,
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
                        //todo   join 改成leftjoin
                .join(Xla_Events.proc(env))
                .where(12,11)
                .equalTo(1,2)
                .with(new JoinFunction<Tuple17<String, String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String>,
                        Tuple3< String,String,String>,
                        Tuple19<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String>>() {
                    @Override
                    public  Tuple19<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String> join(
                            Tuple17<String, String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String> t1,
                            Tuple3< String,String,String> t2) throws Exception {
                        return Tuples.comb(t1,t2,  new int[]{0,1},19);
                    }
                })
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id,
//                 * Hca.Party_Id,  Hca.Account_Number,
//                 * Hp.party_number 客户编码,  Hp.party_name   客户名称,
//                 * Xe.Event_Id, Xe.Application_Id,18
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
                        //todo   join  改成leftjoin
                .join(Xla_Ae_Headers.proc(env))
                .where(18,17)
                .equalTo(1,2)
                .with(new JoinFunction< Tuple19<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String>,
                        Tuple4< String,String,String,String>,
                        Tuple23<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String, String, String,String, String>>() {
                    @Override
                    public  Tuple23<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String, String, String, String, String> join(
                            Tuple19<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String> t1,
                            Tuple4< String,String,String,String> t2) throws Exception {
                        return Tuples.comb(t1,t2,  new int[]{0,1,2,3},23);
                    }
                })
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id,
//                 * Hca.Party_Id,  Hca.Account_Number,
//                 * Hp.party_number 客户编码,  Hp.party_name   客户名称,
//                 * Xe.Event_Id, Xe.Application_Id,18
//                 * Xah.Ae_Header_Id, Xah.Application_Id, Xah.Event_Id,  Xah.Ae_batch_id（upg_batch_id）,22
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
                        //todo join 改成leftjoin
                .join(Xla_Ae_Lines.proc(env))
                .where(19,20)
                .equalTo(0,2)
                .with(new JoinFunction< Tuple23<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String, String, String, String, String>,
                        Tuple7<String,String,String,String,String,String,String>, Row>() {
                    @Override
                        public Row join(
                            Tuple23<String, String, String,String, String, String, String, String, String,  String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
                                Tuple7<String,String,String,String,String,String,String > t2) throws Exception {
                             if (t2 == null) t2 = Tuple7.of("", "", "", "", "", "", "");
                            return Rows.create(t1,t2, new int[]{0,1,2,3,4,5,6});
                        }
                    })
//
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id,
//                 * Hca.Party_Id,  Hca.Account_Number,
//                 * Hp.party_number 客户编码,  Hp.party_name   客户名称,
//                 * Xe.Application_Id, Xe.Entity_Id,18
//                 * Xah.Ae_Header_Id, Xah.Application_Id, Xah.Event_Id,  Xah.Ae_batch_id（upg_batch_id）,22
//                 * Xal.Ae_Header_Id, Xal.Ae_Line_Num,  Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号, 29        21
//                 *
//                 *alhs
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
                        //todo join  改成leftjoin
                .join(AP_XLA_DIS_LINKS.proc(env))
                .where(new Selectors4(new int[]{25,24,23,9}))
                .equalTo(0,1,2,4)
                .with(new JoinFunction<Row,Tuple5< String,String,String,String,String  >, Row>() {
                    @Override
                    public Row join(Row t1, Tuple5< String,String,String,String,String > t2) throws Exception {
                        if (t2 == null) t2 = Tuple5.of("", "", "", "", "");
                        return Rows.create(t1,t2,null);
                    }
                })
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id,
//                 * Hca.Party_Id,  Hca.Account_Number,
//                 * Hp.party_number 客户编码,  Hp.party_name   客户名称,
//                 * Xe.Application_Id, Xe.Entity_Id,18
//                 * Xah.Ae_Header_Id, Xah.Application_Id, Xah.Event_Id,  Xah.Ae_batch_id（upg_batch_id）,22
//                 * Xal.Ae_Header_Id, Xal.Ae_Line_Num,  Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号, 29        21
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
//
                        //todo  join  改成left
                .join(GL_CODE_COMBINATIONS.proc(env))
                .where(new Selector(26))
                .equalTo(0)
                .with(new JoinFunction<Row,Tuple10< String,String,String,String,String, String,String,String,String,String  >, Row>() {
                    @Override
                    public Row join(Row t1, Tuple10< String,String,String,String,String, String,String,String,String,String> t2) throws Exception {
                        if (t2 == null) t2 = Tuple10.of("", "", "", "", "","", "", "", "", "");
                        return Rows.create(t1,t2,new int[]{1,2,3,4,5,6,7,8,9});
                    }
                })
//                /**
//                 * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
//                 * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
//                 * Ada.Line_Id, Ada.Third_Party_Id,10
//                 * Xte.Entity_Id, Xte.Application_Id,
//                 * Hca.Party_Id,  Hca.Account_Number,
//                 * Hp.party_number 客户编码,  Hp.party_name   客户名称,
//                 * Xe.Application_Id, Xe.Entity_Id,18
//                 * Xah.Ae_Header_Id, Xah.Application_Id, Xah.Event_Id,  Xah.Ae_batch_id（upg_batch_id）,22
//                 * Xal.Ae_Header_Id, Xal.Ae_Line_Num,  Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号, 29        21
//                 * gcc.segment1, gcc.segment2, gcc.segment3, gcc.segment4, gcc.segment5, gcc.segment6,  gcc.segment7, gcc.segment8, gcc.segment9, 38
//                 *
//                 * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
//                 *
//                 */
                        //todo join 改成 leftjoin
                .join(FND_USER.proc(env))
                .where(new Selector(1))
                .equalTo(0)
                .with(new JoinFunction<Row,Tuple2< String,String >, Row>() {
                    @Override
                    public Row join(Row t1, Tuple2< String,String> t2) throws Exception {
                        return Rows.create(t1,t2,new int[]{1});
                    }
                }).print();
        }catch (Exception E){
            E.printStackTrace();
        };
        /**
         * select Acra.Cash_Receipt_Id, Acra.CREATED_BY,  Acra.Set_Of_Books_Id, Acra.Receipt_Number, Acra.comments,Acra.Org_Id,
         * Acrha.Cash_Receipt_History_Id, Acrha.Cash_Receipt_Id, Acrha.gl_date,8
         * Ada.Line_Id, Ada.Third_Party_Id,10
         * Xte.Entity_Id, Xte.Application_Id,
         * Hca.Party_Id,  Hca.Account_Number,14
         * Hp.party_number 客户编码,  Hp.party_name   客户名称,16
         * Xe.Application_Id, Xe.Entity_Id,18
         * Xah.Ae_Header_Id, Xah.Application_Id, Xah.Event_Id,  Xah.Ae_batch_id（upg_batch_id）,22
         * Xal.Ae_Header_Id, Xal.Ae_Line_Num,  Xal.Application_Id, Xal.CODE_COMBINATION_ID, Xal.ACCOUNTED_DR 借项, Xal.ACCOUNTED_CR 贷项, Xal.USSGL_TRANSACTION_CODE 凭证编号, 29        21
         * gcc.segment1, gcc.segment2, gcc.segment3, gcc.segment4, gcc.segment5, gcc.segment6,  gcc.segment7, gcc.segment8, gcc.segment9,
         * fu.USER_NAME
         *
         * from AR_CASH_RECEIPTS_ALL (join AR_CASH_RECEIPT_HISTORY_ALL on  Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id)
         *
         */
        return ds;
    }
}

/**
 * 收款  SLA与AR_RECEIPTS
 *
 *   Select Acra.Org_Id,
 *            Acra.Receipt_Number 业务系统单据编号,
 *            Acrha.gl_date 会计期间,
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
 *            Acra.comments 备注
 *       From Ar_Cash_Receipts_All         Acra,
 *            Ar_Cash_Receipt_History_All  Acrha,
 *            Ar_Distributions_All         Ada,
 *            Hz_Cust_Accounts             Hca,
 *            Hz_Parties                   Hp,
 *            Xla.Xla_Transaction_Entities Xte,
 *            Xla_Events                   Xe,
 *            Xla_Ae_Headers               Xah,
 *            Xla_Ae_Lines                 Xal,
 *            Xla_Distribution_Links       Xdl,
 *            gl_code_combinations_01      GCC,
 *            FND_USER_01                  FU
 *      Where Acra.Cash_Receipt_Id = Acrha.Cash_Receipt_Id
 *        And Acrha.Cash_Receipt_History_Id = Ada.Source_Id
 *        And Ada.Source_Table in ('CRH','RA','MCD')
 *        And Hca.Cust_Account_Id(+) = Ada.Third_Party_Id
 *        And Hp.Party_Id(+) = Hca.Party_Id
 *        And Xte.Ledger_Id = Acra.Set_Of_Books_Id
 *        And Xte.Entity_Code = 'RECEIPTS'
 *        And Nvl(Xte.Source_Id_Int_1, -99) = Acra.Cash_Receipt_Id
 *        And Xte.Application_Id = Xe.Application_Id
 *        And Xte.Entity_Id = Xe.Entity_Id
 *        And Xe.Application_Id = Xah.Application_Id
 *        And Xe.Event_Id = Xah.Event_Id
 *        And Xah.Application_Id = Xal.Application_Id
 *        And Xah.Ae_Header_Id = Xal.Ae_Header_Id
 *        And Xal.Application_Id = Xdl.Application_Id
 *        And Xal.Ae_Header_Id = Xdl.Ae_Header_Id
 *        And Xal.Ae_Line_Num = Xdl.Ae_Line_Num
 *        And Xdl.Source_Distribution_Type = 'AR_DISTRIBUTIONS_ALL'
 *        And Xdl.Source_Distribution_Id_Num_1 = Ada.Line_Id
 *        and Xal.CODE_COMBINATION_ID=GCC.CODE_COMBINATION_ID
 *        and Acra.CREATED_BY = FU.USER_ID
 *
 */
