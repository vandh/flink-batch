/*****************************************DW�ṩ��ԭʼ�汾****************************************/
 SELECT AID.ACCOUNTING_DATE ��������,
        AI.SOURCE ��Դ,
        DECODE(AI.SOURCE, 'EAS', AB.BATCH_NAME, 'SMP', AB.BATCH_NAME, NULL) ����,
        AI.ATTRIBUTE4 ҵ��ϵͳ���ݱ��,
        PV.SEGMENT1 ��Ӧ�̱��,
        PV.VENDOR_NAME ��Ӧ������,
        PVS.VENDOR_SITE_CODE ��Ӧ�̵ص�,
        AB.BATCH_NAME ����,
        AI.INVOICE_NUM ��Ʊ���,
        GCC.CONCATENATED_SEGMENTS �˻�,
        AEL.USSGL_TRANSACTION_CODE ƾ֤���,
        AEL.ACCOUNTED_DR ����,
        AEL.ACCOUNTED_CR ����,
        FU.USER_NAME   �Ƶ���,        
        AI.DESCRIPTION ����
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
/*****************************************���Ӧ����Ʊ��Ĺ��̰汾�������ֶ�˵�������ڱ��飩****************************************/
 SELECT 
    NULL 'ID',
    null 'ʡ��ID',
    AI.ORG_ID '��֯����ID',
    AI.gl_date '����',
    AID.ACCOUNTING_DATE '��������',
    AI.SOURCE  '��Դϵͳ����',
    null '��Դϵͳ',
    DECODE(AI.SOURCE, 'EAS', AB.BATCH_NAME, 'SMP', AB.BATCH_NAME, NULL) '����',
    AI.ATTRIBUTE4 'ҵ��ϵͳ���ݱ��',
    PV.SEGMENT1 '��Ӧ�̱��',
    PV.VENDOR_NAME '��Ӧ������',
    PVS.VENDOR_SITE_CODE '��Ӧ�̵ص�',
    AI.INVOICE_NUM '��Ʊ���',
    gcc.SEGMENT1 || '.' || gcc.SEGMENT2 || '.' || gcc.SEGMENT3 || '.' ||
    gcc.SEGMENT4 || '.' || gcc.SEGMENT5 || '.' || gcc.SEGMENT6 || '.' ||
    gcc.SEGMENT7 || '.' || gcc.SEGMENT8 || '.' || gcc.SEGMENT9  '�˻����',
    null '�˻�����',
    gcc.SEGMENT1 '��˾��ֵ',
    null '��˾������'
    gcc.SEGMENT2 '�ɱ����Ķ�ֵ',
    null '�ɱ����Ķ�����'
    gcc.SEGMENT3 'רҵ��ֵ',
    null 'רҵ������'
    gcc.SEGMENT4 '��Ŀ��ֵ',
    null '��Ŀ������'
    gcc.SEGMENT5 '������ֵ',
    null '����������'
    gcc.SEGMENT6 '��Ŀ��ֵ',
    null '��Ŀ������'
    gcc.SEGMENT7 '�ͻ���ֵ',
    null '�ͻ�������'
    gcc.SEGMENT8 '���ö�1',
    null '���ö�1����'
    gcc.SEGMENT9 '���ö�2',
    null '���ö�2����',
    AEL.USSGL_TRANSACTION_CODE  'ƾ֤���',
    AEL.ACCOUNTED_DR ����,
    AEL.ACCOUNTED_CR ����,
    null '���ֱ���',
    null '����',
    null '����',
    null '��ҽ��',
    null 'ȫ�ɱ�ָ�����',
    null 'ȫ�ɱ�ָ������',
    FU.USER_NAME '�Ƶ���',
    AI.DESCRIPTION '����'
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


/*****************************************���Ӧ����Ʊ��Ĺ��̰汾�������ֶ�˵�������ڱ��飩****************************************/
һ������Ӧ����Ʊ
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
    
��������Ԥ���Ʊ
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