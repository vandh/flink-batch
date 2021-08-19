/*****************************************DW提供的原始版本****************************************/
 SELECT AID.ACCOUNTING_DATE 入账日期,
        AI.SOURCE 来源,
        DECODE(AI.SOURCE, 'EAS', AB.BATCH_NAME, 'SMP', AB.BATCH_NAME, NULL) 批名,
        AI.ATTRIBUTE4 业务系统单据编号,
        PV.SEGMENT1 供应商编号,
        PV.VENDOR_NAME 供应商名称,
        PVS.VENDOR_SITE_CODE 供应商地点,
        AB.BATCH_NAME 批名,
        AI.INVOICE_NUM 发票编号,
        GCC.CONCATENATED_SEGMENTS 账户,
        AEL.USSGL_TRANSACTION_CODE 凭证编号,
        AEL.ACCOUNTED_DR 借项,
        AEL.ACCOUNTED_CR 贷项,
        FU.USER_NAME   制单人,        
        AI.DESCRIPTION 描述
   FROM AP_BATCHES_ALL               AB,
        AP.AP_INVOICES_ALL           AI,
        AP_INVOICE_DISTRIBUTIONS_ALL AID,
        APPS.PO_VENDOR_SITES_ALL     PVS,
        APPS.PO_VENDORS              PV,
        XLA.XLA_AE_HEADERS           AEH,
        XLA.XLA_AE_LINES             AEL,
        XLA.XLA_DISTRIBUTION_LINKS   XDL,
        XLA.XLA_TRANSACTION_ENTITIES XTE,
        GL_CODE_COMBINATIONSLE_KFV   GCC,
        FND_USER                     FU
  WHERE AI.VENDOR_ID = PV.VENDOR_ID
    AND AI.INVOICE_ID = AID.INVOICE_ID
    AND AI.BATCH_ID = AB.BATCH_ID
    AND AID.INVOICE_DISTRIBUTION_ID = XDL.SOURCE_DISTRIBUTION_ID_NUM_1
    AND XDL.APPLICATION_ID = AEH.APPLICATION_ID
    AND XDL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND XDL.AE_LINE_NUM = AEL.AE_LINE_NUM
    AND AI.VENDOR_SITE_ID = PVS.VENDOR_SITE_ID
    AND AEH.ENTITY_ID = XTE.ENTITY_ID
    AND XTE.SOURCE_ID_INT_1 = AI.INVOICE_ID
    AND XTE.SECURITY_ID_INT_1 = AI.ORG_ID
    AND XTE.ENTITY_CODE IN ('AP_INVOICES', 'AP_INVOICE_DISTRIBUTIONS')
    AND XDL.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST'
       --   AND AEL.ACCOUNTING_CLASS_CODE = 'LIABILITY'
    AND AEL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND AEH.APPLICATION_ID = 200
    AND AEH.APPLICATION_ID = AEL.APPLICATION_ID
    AND AEL.CODE_COMBINATION_ID = GCC.CODE_COMBINATION_ID
    AND AI.CREATED_BY = FU.USER_ID
    AND AID.ACCOUNTING_DATE BETWEEN TO_DATE('2021-02-01', 'YYYY-MM-DD') AND
        TO_DATE('2021-02-28', 'YYYY-MM-DD')
    AND AI.ORG_ID = 159
/*****************************************针对应付发票表的过程版本（含有字段说明，用于备查）****************************************/
 SELECT 
    NULL 'ID',
    null '省分ID',
    AI.ORG_ID '组织机构ID',
    AI.gl_date '账期',
    AID.ACCOUNTING_DATE '入账日期',
    AI.SOURCE  '来源系统编码',
    null '来源系统',
    DECODE(AI.SOURCE, 'EAS', AB.BATCH_NAME, 'SMP', AB.BATCH_NAME, NULL) '批名',
    AI.ATTRIBUTE4 '业务系统单据编号',
    PV.SEGMENT1 '供应商编号',
    PV.VENDOR_NAME '供应商名称',
    PVS.VENDOR_SITE_CODE '供应商地点',
    AI.INVOICE_NUM '发票编号',
    gcc.SEGMENT1 || '.' || gcc.SEGMENT2 || '.' || gcc.SEGMENT3 || '.' ||
    gcc.SEGMENT4 || '.' || gcc.SEGMENT5 || '.' || gcc.SEGMENT6 || '.' ||
    gcc.SEGMENT7 || '.' || gcc.SEGMENT8 || '.' || gcc.SEGMENT9  '账户组合',
    null '账户名称',
    gcc.SEGMENT1 '公司段值',
    null '公司段名称'
    gcc.SEGMENT2 '成本中心段值',
    null '成本中心段名称'
    gcc.SEGMENT3 '专业段值',
    null '专业段名称'
    gcc.SEGMENT4 '科目段值',
    null '科目段名称'
    gcc.SEGMENT5 '往来段值',
    null '往来段名称'
    gcc.SEGMENT6 '项目段值',
    null '项目段名称'
    gcc.SEGMENT7 '客户段值',
    null '客户段名称'
    gcc.SEGMENT8 '备用段1',
    null '备用段1名称'
    gcc.SEGMENT9 '备用段2',
    null '备用段2名称',
    AEL.USSGL_TRANSACTION_CODE  '凭证编号',
    AEL.ACCOUNTED_DR 借项,
    AEL.ACCOUNTED_CR 贷项,
    null '币种编码',
    null '币种',
    null '汇率',
    null '外币金额',
    null '全成本指标编码',
    null '全成本指标名称',
    FU.USER_NAME '制单人',
    AI.DESCRIPTION '描述'
   FROM AP_BATCHES_ALL_01               AB,
        AP_INVOICE_STD_01(ap_invoice_pre_01)  AI,
        AP_INVOICE_DIST_STD_01(AP_INVOICE_DIST_pre_01) AID,
        PO_VENDOR_SITES_ALL_01       PVS,
        PO_VENDORS_01                PV,
        AP_XLA_AE_HEADERS_01         AEH,
        AP_XLA_AE_LINES_01           AEL,
        AP_XLA_DIS_LINKS_01          XDL,
        AP_XLA_TRX_ENTITIES_01       XTE,
        GL_CODE_COMBINATIONS_01   GCC,
        FND_USER_01                  FU
  WHERE AI.VENDOR_ID = PV.VENDOR_ID
    AND AI.INVOICE_ID = AID.INVOICE_ID
    AND AI.BATCH_ID = AB.BATCH_ID
    AND AID.INVOICE_DISTRIBUTION_ID = XDL.SOURCE_DISTRIBUTION_ID_NUM_1
    AND XDL.APPLICATION_ID = AEH.APPLICATION_ID
    AND XDL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND XDL.AE_LINE_NUM = AEL.AE_LINE_NUM
    AND AI.VENDOR_SITE_ID = PVS.VENDOR_SITE_ID
    AND AEH.ENTITY_ID = XTE.ENTITY_ID
    AND XTE.SOURCE_ID_INT_1 = AI.INVOICE_ID
    AND XTE.SECURITY_ID_INT_1 = AI.ORG_ID
    AND XTE.ENTITY_CODE IN ('AP_INVOICES', 'AP_INVOICE_DISTRIBUTIONS')
    AND XDL.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST'
       --   AND AEL.ACCOUNTING_CLASS_CODE = 'LIABILITY'
    AND AEL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND AEH.APPLICATION_ID = 200
    AND AEH.APPLICATION_ID = AEL.APPLICATION_ID
    AND AEL.CODE_COMBINATION_ID = GCC.CODE_COMBINATION_ID
    AND AI.CREATED_BY = FU.USER_ID
    AND AID.ACCOUNTING_DATE BETWEEN TO_DATE('2021-02-01', 'YYYY-MM-DD') AND
        TO_DATE('2021-02-28', 'YYYY-MM-DD')
    AND AI.ORG_ID = 159


/*****************************************针对应付发票表的过程版本（含有字段说明，用于备查）****************************************/
一、处理应付发票
 SELECT 
    NULL,
    null,
    AI.ORG_ID,
    AI.gl_date,
    AID.ACCOUNTING_DATE,
    AI.SOURCE,
    null,
    AB.BATCH_NAME,
    AI.ATTRIBUTE4,
    PV.SEGMENT1,
    PV.VENDOR_NAME,
    PVS.VENDOR_SITE_CODE,
    AI.INVOICE_NUM,
    gcc.SEGMENT1 || '.' || gcc.SEGMENT2 || '.' || gcc.SEGMENT3 || '.' ||
    gcc.SEGMENT4 || '.' || gcc.SEGMENT5 || '.' || gcc.SEGMENT6 || '.' ||
    gcc.SEGMENT7 || '.' || gcc.SEGMENT8 || '.' || gcc.SEGMENT9,
    null,
    gcc.SEGMENT1,
    null
    gcc.SEGMENT2,
    null,
    gcc.SEGMENT3,
    null,
    gcc.SEGMENT4,
    null,
    gcc.SEGMENT5,
    null,
    gcc.SEGMENT6,
    null,
    gcc.SEGMENT7,
    null,
    gcc.SEGMENT8,
    null,
    gcc.SEGMENT9,
    null,
    AEL.USSGL_TRANSACTION_CODE,
    AEL.ACCOUNTED_DR,
    AEL.ACCOUNTED_CR,
    null,
    null,
    null,
    null,
    null,
    null,
    FU.USER_NAME,
    AI.DESCRIPTION
   FROM AP_BATCHES_ALL_01            AB,
        AP_INVOICE_STD_01            AI,
        AP_INVOICE_DIST_STD_01       AID,
        PO_VENDOR_SITES_ALL_01       PVS,
        PO_VENDORS_01                PV,
        AP_XLA_AE_HEADERS_01         AEH,
        AP_XLA_AE_LINES_01           AEL,
        AP_XLA_DIS_LINKS_01          XDL,
        AP_XLA_TRX_ENTITIES_01       XTE,
        GL_CODE_COMBINATIONS_01      GCC,
        FND_USER_01                  FU
  WHERE AI.VENDOR_ID = PV.VENDOR_ID
    AND AI.INVOICE_ID = AID.INVOICE_ID
    AND AI.BATCH_ID = AB.BATCH_ID
    AND AID.INVOICE_DISTRIBUTION_ID = XDL.SOURCE_DISTRIBUTION_ID_NUM_1
    AND XDL.APPLICATION_ID = AEH.APPLICATION_ID
    AND XDL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND XDL.AE_LINE_NUM = AEL.AE_LINE_NUM
    AND AI.VENDOR_SITE_ID = PVS.VENDOR_SITE_ID
    AND AEH.ENTITY_ID = XTE.ENTITY_ID
    AND XTE.SOURCE_ID_INT_1 = AI.INVOICE_ID
    AND XTE.SECURITY_ID_INT_1 = AI.ORG_ID
    AND XTE.ENTITY_CODE IN ('AP_INVOICES', 'AP_INVOICE_DISTRIBUTIONS')
    AND XDL.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST'
       --   AND AEL.ACCOUNTING_CLASS_CODE = 'LIABILITY'
    AND AEL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND AEH.APPLICATION_ID = 200
    AND AEH.APPLICATION_ID = AEL.APPLICATION_ID
    AND AEL.CODE_COMBINATION_ID = GCC.CODE_COMBINATION_ID
    AND AI.CREATED_BY = FU.USER_ID
    
    
    AND AID.ACCOUNTING_DATE BETWEEN TO_DATE('2021-02-01', 'YYYY-MM-DD') AND
        TO_DATE('2021-02-28', 'YYYY-MM-DD')
    AND AI.ORG_ID = 159
    
二、处理预付款发票
 SELECT 
    NULL,
    null,
    AI.ORG_ID,
    AI.gl_date,
    AID.ACCOUNTING_DATE,
    AI.SOURCE,
    null,
    AB.BATCH_NAME,
    AI.ATTRIBUTE4,
    PV.SEGMENT1,
    PV.VENDOR_NAME,
    PVS.VENDOR_SITE_CODE,
    AI.INVOICE_NUM,
    gcc.SEGMENT1 || '.' || gcc.SEGMENT2 || '.' || gcc.SEGMENT3 || '.' ||
    gcc.SEGMENT4 || '.' || gcc.SEGMENT5 || '.' || gcc.SEGMENT6 || '.' ||
    gcc.SEGMENT7 || '.' || gcc.SEGMENT8 || '.' || gcc.SEGMENT9,
    null,
    gcc.SEGMENT1,
    null
    gcc.SEGMENT2,
    null,
    gcc.SEGMENT3,
    null,
    gcc.SEGMENT4,
    null,
    gcc.SEGMENT5,
    null,
    gcc.SEGMENT6,
    null,
    gcc.SEGMENT7,
    null,
    gcc.SEGMENT8,
    null,
    gcc.SEGMENT9,
    null,
    AEL.USSGL_TRANSACTION_CODE,
    AEL.ACCOUNTED_DR,
    AEL.ACCOUNTED_CR,
    null,
    null,
    null,
    null,
    null,
    null,
    FU.USER_NAME,
    AI.DESCRIPTION
   FROM AP_BATCHES_ALL_01            AB,
        ap_invoice_pre_01            AI,
        AP_INVOICE_DIST_pre_01       AID,
        PO_VENDOR_SITES_ALL_01       PVS,
        PO_VENDORS_01                PV,
        AP_XLA_AE_HEADERS_01         AEH,
        AP_XLA_AE_LINES_01           AEL,
        AP_XLA_DIS_LINKS_01          XDL,
        AP_XLA_TRX_ENTITIES_01       XTE,
        GL_CODE_COMBINATIONS_01      GCC,
        FND_USER_01                  FU
  WHERE AI.VENDOR_ID = PV.VENDOR_ID
    AND AI.INVOICE_ID = AID.INVOICE_ID
    AND AI.BATCH_ID = AB.BATCH_ID
    AND AID.INVOICE_DISTRIBUTION_ID = XDL.SOURCE_DISTRIBUTION_ID_NUM_1
    AND XDL.APPLICATION_ID = AEH.APPLICATION_ID
    AND XDL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND XDL.AE_LINE_NUM = AEL.AE_LINE_NUM
    AND AI.VENDOR_SITE_ID = PVS.VENDOR_SITE_ID
    AND AEH.ENTITY_ID = XTE.ENTITY_ID
    AND XTE.SOURCE_ID_INT_1 = AI.INVOICE_ID
    AND XTE.SECURITY_ID_INT_1 = AI.ORG_ID
    AND XTE.ENTITY_CODE IN ('AP_INVOICES', 'AP_INVOICE_DISTRIBUTIONS')
    AND XDL.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST'
       --   AND AEL.ACCOUNTING_CLASS_CODE = 'LIABILITY'
    AND AEL.AE_HEADER_ID = AEH.AE_HEADER_ID
    AND AEH.APPLICATION_ID = 200
    AND AEH.APPLICATION_ID = AEL.APPLICATION_ID
    AND AEL.CODE_COMBINATION_ID = GCC.CODE_COMBINATION_ID
    AND AI.CREATED_BY = FU.USER_ID
    AND AID.ACCOUNTING_DATE BETWEEN TO_DATE('2021-02-01', 'YYYY-MM-DD') AND
        TO_DATE('2021-02-28', 'YYYY-MM-DD')
    AND AI.ORG_ID = 159        