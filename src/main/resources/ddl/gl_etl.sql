select
    gjh.je_header_id,
    qsv.sequence_value  "凭证号",
    gj.default_period_name "期间",
    js.user_je_source_name  "来源",
    gj.name "批名",
    jc.user_je_category_name "类别",
    gj.creation_date "创建日期",
    qgla.post_date "过帐日期",
    fu.user_name "创建人",
    CONCAT(gcc.segment1,'.',gcc.segment2,'.',gcc.segment3,'.',gcc.segment4,'.',gcc.segment5,'.',gcc.segment6,'.',gcc.segment7,'.',gcc.segment8,'.',gcc.segment9) "账户组合",
       '' "账户说明",
       gcc.segment1 公司段,
       gcc.segment2 成本中心段,
       gcc.segment3 专业段,
       gcc.segment4 科目段,
       gcc.segment5 往来段,
       gcc.segment6 项目段,
       gcc.segment7 客户段,
       gcc.segment8 备用段1,
       gcc.segment9 备用段2,
       GJH.doc_sequence_value 凭证编号,
       gjl.entered_dr "输入借方金额",
       gjl.entered_cr "输入贷方金额",
       gjl.accounted_dr "入账借方金额",
       gjl.accounted_cr "入账贷方金额",
       GJH.Currency_Code "币种",
       GJH.currency_conversion_rate 汇率,
       0 外币金额,
       '' 全成本指标,
       gjl.reference_9 全成本编码,
        gjh.description "凭证摘要",
        gjh.name "日记帐名",
       gjl.je_line_num "行号",
       gjl.description "摘要"
  from gl_je_batches_post_01 gj join gl_je_headers_post_01 GJH
       on(gj.je_batch_id = gjh.je_batch_id and GJH.actual_flag = 'A' and GJH.period_name in('2021-01','2021-02','2021-03','2021-04','2021-05'))
       join gl_je_lines_post_01 gjl on (gjh.je_header_id = gjl.je_header_id)
       join gl_code_combinations_01 gcc on (gcc.CODE_COMBINATION_ID = gjl.code_combination_id and GCC.SUMMARY_FLAG = 'N')
       join qgl_sequence_value_01 qsv on (gjh.je_header_id = qsv.je_header_id)
       join gl_je_categories_tl_01 jc on (jc.je_category_name = gjh.je_category and jc.language='ZHS')
       join GL_JE_SOURCES_TL_01 js on (js.je_source_name = gjh.je_source and js.language='ZHS')
       left join fnd_user_01 fu on(fu.user_id = gj.created_by)
       left join qgl_approve_01 qgla on(gjh.je_header_id = qgla.je_header_id);
 -- where  GJH.actual_flag = 'A' and GJH.period_name='2021-01';

