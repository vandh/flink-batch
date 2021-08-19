-- 应收发票：

Select Rcta.Org_Id,
       Rcta.Trx_Number 业务系统单据编号,
       Rctlgda.gl_posted_date 会计期间,
       Xah.Ae_batch_id Je_batch_id,
       Xah.Ae_Header_Id Je_Header_Id,
       Xal.Ae_Line_Num Je_Line_Num,
       Hp.party_number 客户编码,
       Hp.party_name   客户名称,
       Hca.Account_Number 客户账户,
       Xal.USSGL_TRANSACTION_CODE 凭证编号,
       CONCAT(gcc.segment1,'.',gcc.segment2,'.',gcc.segment3,'.',gcc.segment4,'.',gcc.segment5,'.',gcc.segment6,'.',gcc.segment7,'.',gcc.segment8,'.',gcc.segment9) 科目组合,
       gcc.segment1,
       gcc.segment2,
       gcc.segment3,
       gcc.segment4,
       gcc.segment5,
       gcc.segment6,
       gcc.segment7,
       gcc.segment8,
       gcc.segment9,
       Xal.ACCOUNTED_DR 借项,
       Xal.ACCOUNTED_CR 贷项,
       fu.USER_NAME 制单人,
       Rctla.description 摘要
From Ra_Customer_Trx_All          Rcta,  -- /data/20210709/  /data/day/20210715/
     Ra_Customer_Trx_Lines_All    Rctla,
     Ra_Cust_Trx_Line_Gl_Dist_All Rctlgda,
     Hz_Cust_Accounts             Hca,
     Hz_Parties                   Hp,
     AP_XLA_TRX_ENTITIES Xte,
     Xla_Events                   Xe,
     AP_XLA_AE_HEADERS               Xah,
     AP_XLA_AE_LINES                 Xal,
     AP_XLA_DIS_LINKS       Xdl,
     gl_code_combinations_01          GCC,
     FND_USER_01                      FU
Where Rcta.Customer_Trx_Id = Rctlgda.Customer_Trx_Id
  And Rctlgda.Customer_Trx_Id = Rctla.Customer_Trx_Id(+)
  And Rctlgda.Customer_Trx_Line_Id = Rctla.Customer_Trx_Line_Id(+)
  And Hca.Cust_Account_Id(+) = Rcta.Bill_To_Customer_Id
  And Hp.Party_Id(+) = Hca.Party_Id
  And Xte.Ledger_Id = Rcta.Set_Of_Books_Id
  And Xte.Entity_Code = 'TRANSACTIONS'
  And Nvl(Xte.Source_Id_Int_1, -99) = Rcta.Customer_Trx_Id
  And Xte.Application_Id = Xe.Application_Id
  And Xte.Entity_Id = Xe.Entity_Id
  And Xe.Application_Id = Xah.Application_Id
  And Xe.Event_Id = Xah.Event_Id
  And Xah.Application_Id = Xal.Application_Id
  And Xah.Ae_Header_Id = Xal.Ae_Header_Id
  And Xal.Application_Id = Xdl.Application_Id
  And Xal.Ae_Header_Id = Xdl.Ae_Header_Id
  And Xal.Ae_Line_Num = Xdl.Ae_Line_Num
  And Xdl.Source_Distribution_Type = 'RA_CUST_TRX_LINE_GL_DIST_ALL'
  And Xdl.Source_Distribution_Id_Num_1 = Rctlgda.Cust_Trx_Line_Gl_Dist_Id
  and Xal.CODE_COMBINATION_ID=GCC.CODE_COMBINATION_ID
  AND AI.CREATED_BY = FU.USER_ID







