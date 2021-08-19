create table NODS_ERP.RA_CUSTOMER_TRX_ALL_01
(
  customer_trx_id                NUMBER(15) not null,
  last_update_date               DATE not null,
  last_updated_by                NUMBER(15) not null,
  creation_date                  DATE not null,
  created_by                     NUMBER(15) not null,
  last_update_login              NUMBER(15),
  trx_number                     VARCHAR2(20) not null,
  cust_trx_type_id               NUMBER(15) not null,
  trx_date                       DATE not null,
  set_of_books_id                NUMBER(15) not null,
  bill_to_contact_id             NUMBER(15),
  batch_id                       NUMBER(15),
  batch_source_id                NUMBER(15),
  reason_code                    VARCHAR2(30),
  sold_to_customer_id            NUMBER(15),
  sold_to_contact_id             NUMBER(15),
  sold_to_site_use_id            NUMBER(15),
  bill_to_customer_id            NUMBER(15),
  bill_to_site_use_id            NUMBER(15),
  ship_to_customer_id            NUMBER(15),
  ship_to_contact_id             NUMBER(15),
  ship_to_site_use_id            NUMBER(15),
  shipment_id                    NUMBER(15),
  remit_to_address_id            NUMBER(15),
  term_id                        NUMBER(15),
  term_due_date                  DATE,
  previous_customer_trx_id       NUMBER(15),
  primary_salesrep_id            NUMBER(15),
  printing_original_date         DATE,
  printing_last_printed          DATE,
  printing_option                VARCHAR2(20),
  printing_count                 NUMBER(15),
  printing_pending               VARCHAR2(1),
  purchase_order                 VARCHAR2(50),
  purchase_order_revision        VARCHAR2(50),
  purchase_order_date            DATE,
  customer_reference             VARCHAR2(30),
  customer_reference_date        DATE,
  comments                       VARCHAR2(1760),
  internal_notes                 VARCHAR2(240),
  exchange_rate_type             VARCHAR2(30),
  exchange_date                  DATE,
  exchange_rate                  NUMBER,
  territory_id                   NUMBER(15),
  invoice_currency_code          VARCHAR2(15),
  initial_customer_trx_id        NUMBER(15),
  agreement_id                   NUMBER(15),
  end_date_commitment            DATE,
  start_date_commitment          DATE,
  last_printed_sequence_num      NUMBER(15),
  attribute_category             VARCHAR2(30),
  attribute1                     VARCHAR2(150),
  attribute2                     VARCHAR2(150),
  attribute3                     VARCHAR2(150),
  attribute4                     VARCHAR2(150),
  attribute5                     VARCHAR2(150),
  attribute6                     VARCHAR2(150),
  attribute7                     VARCHAR2(150),
  attribute8                     VARCHAR2(150),
  attribute9                     VARCHAR2(150),
  attribute10                    VARCHAR2(150),
  orig_system_batch_name         VARCHAR2(40),
  post_request_id                NUMBER(15),
  request_id                     NUMBER(15),
  program_application_id         NUMBER(15),
  program_id                     NUMBER(15),
  program_update_date            DATE,
  finance_charges                VARCHAR2(1),
  complete_flag                  VARCHAR2(1) not null,
  posting_control_id             NUMBER(15),
  bill_to_address_id             NUMBER(15),
  ra_post_loop_number            NUMBER(15),
  ship_to_address_id             NUMBER(15),
  credit_method_for_rules        VARCHAR2(30),
  credit_method_for_installments VARCHAR2(30),
  receipt_method_id              NUMBER(15),
  attribute11                    VARCHAR2(150),
  attribute12                    VARCHAR2(150),
  attribute13                    VARCHAR2(150),
  attribute14                    VARCHAR2(150),
  attribute15                    VARCHAR2(150),
  related_customer_trx_id        NUMBER(15),
  invoicing_rule_id              NUMBER(15),
  ship_via                       VARCHAR2(30),
  ship_date_actual               DATE,
  waybill_number                 VARCHAR2(50),
  fob_point                      VARCHAR2(30),
  customer_bank_account_id       NUMBER(15),
  interface_header_attribute1    VARCHAR2(150),
  interface_header_attribute2    VARCHAR2(150),
  interface_header_attribute3    VARCHAR2(150),
  interface_header_attribute4    VARCHAR2(150),
  interface_header_attribute5    VARCHAR2(150),
  interface_header_attribute6    VARCHAR2(150),
  interface_header_attribute7    VARCHAR2(150),
  interface_header_attribute8    VARCHAR2(150),
  interface_header_context       VARCHAR2(30),
  default_ussgl_trx_code_context VARCHAR2(30),
  interface_header_attribute10   VARCHAR2(150),
  interface_header_attribute11   VARCHAR2(150),
  interface_header_attribute12   VARCHAR2(150),
  interface_header_attribute13   VARCHAR2(150),
  interface_header_attribute14   VARCHAR2(150),
  interface_header_attribute15   VARCHAR2(150),
  interface_header_attribute9    VARCHAR2(150),
  default_ussgl_transaction_code VARCHAR2(30),
  recurred_from_trx_number       VARCHAR2(20),
  status_trx                     VARCHAR2(30),
  doc_sequence_id                NUMBER(15),
  doc_sequence_value             NUMBER(15),
  paying_customer_id             NUMBER(15),
  paying_site_use_id             NUMBER(15),
  related_batch_source_id        NUMBER(15),
  default_tax_exempt_flag        VARCHAR2(1),
  created_from                   VARCHAR2(30) not null,
  org_id                         NUMBER(15),
  wh_update_date                 DATE,
  global_attribute1              VARCHAR2(150),
  global_attribute2              VARCHAR2(150),
  global_attribute3              VARCHAR2(150),
  global_attribute4              VARCHAR2(150),
  global_attribute5              VARCHAR2(150),
  global_attribute6              VARCHAR2(150),
  global_attribute7              VARCHAR2(150),
  global_attribute8              VARCHAR2(150),
  global_attribute9              VARCHAR2(150),
  global_attribute10             VARCHAR2(150),
  global_attribute11             VARCHAR2(150),
  global_attribute12             VARCHAR2(150),
  global_attribute13             VARCHAR2(150),
  global_attribute14             VARCHAR2(150),
  global_attribute15             VARCHAR2(150),
  global_attribute16             VARCHAR2(150),
  global_attribute17             VARCHAR2(150),
  global_attribute18             VARCHAR2(150),
  global_attribute19             VARCHAR2(150),
  global_attribute20             VARCHAR2(150),
  global_attribute21             VARCHAR2(150),
  global_attribute22             VARCHAR2(150),
  global_attribute23             VARCHAR2(150),
  global_attribute24             VARCHAR2(150),
  global_attribute25             VARCHAR2(150),
  global_attribute26             VARCHAR2(150),
  global_attribute27             VARCHAR2(150),
  global_attribute28             VARCHAR2(150),
  global_attribute29             VARCHAR2(150),
  global_attribute30             VARCHAR2(150),
  global_attribute_category      VARCHAR2(30),
  edi_processed_flag             VARCHAR2(1),
  edi_processed_status           VARCHAR2(10),
  mrc_exchange_rate_type         VARCHAR2(2000),
  mrc_exchange_date              VARCHAR2(2000),
  mrc_exchange_rate              VARCHAR2(2000),
  payment_server_order_num       VARCHAR2(80),
  approval_code                  VARCHAR2(80),
  address_verification_code      VARCHAR2(80),
  old_trx_number                 VARCHAR2(20),
  br_amount                      NUMBER,
  br_unpaid_flag                 VARCHAR2(1),
  br_on_hold_flag                VARCHAR2(1),
  drawee_id                      NUMBER(15),
  drawee_contact_id              NUMBER(15),
  drawee_site_use_id             NUMBER(15),
  remittance_bank_account_id     NUMBER(15),
  override_remit_account_flag    VARCHAR2(1),
  drawee_bank_account_id         NUMBER(15),
  special_instructions           VARCHAR2(240),
  remittance_batch_id            NUMBER(15),
  prepayment_flag                VARCHAR2(1),
  ct_reference                   VARCHAR2(150),
  contract_id                    NUMBER,
  bill_template_id               NUMBER(15),
  reversed_cash_receipt_id       NUMBER(15),
  cc_error_code                  VARCHAR2(80),
  cc_error_text                  VARCHAR2(255),
  cc_error_flag                  VARCHAR2(1),
  upgrade_method                 VARCHAR2(30),
  legal_entity_id                NUMBER(15),
  remit_bank_acct_use_id         NUMBER(15),
  payment_trxn_extension_id      NUMBER(15),
  ax_accounted_flag              VARCHAR2(1),
  application_id                 NUMBER(15),
  payment_attributes             VARCHAR2(1000),
  billing_date                   DATE,
  interest_header_id             NUMBER(15),
  late_charges_assessed          VARCHAR2(30),
  trailer_number                 VARCHAR2(50),
  ods_creation_date              DATE
)


create table NODS_ERP.RA_CUSTOMER_TRX_LINES_ALL_01
(
  customer_trx_line_id           NUMBER(15) not null,
  last_update_date               DATE not null,
  last_updated_by                NUMBER(15) not null,
  creation_date                  DATE not null,
  created_by                     NUMBER(15) not null,
  last_update_login              NUMBER(15),
  customer_trx_id                NUMBER(15) not null,
  line_number                    NUMBER not null,
  set_of_books_id                NUMBER(15) not null,
  reason_code                    VARCHAR2(30),
  inventory_item_id              NUMBER(15),
  description                    VARCHAR2(240),
  previous_customer_trx_id       NUMBER(15),
  previous_customer_trx_line_id  NUMBER(15),
  quantity_ordered               NUMBER,
  quantity_credited              NUMBER,
  quantity_invoiced              NUMBER,
  unit_standard_price            NUMBER,
  unit_selling_price             NUMBER,
  sales_order                    VARCHAR2(50),
  sales_order_revision           NUMBER,
  sales_order_line               VARCHAR2(30),
  sales_order_date               DATE,
  accounting_rule_id             NUMBER(15),
  accounting_rule_duration       NUMBER(15),
  line_type                      VARCHAR2(20) not null,
  attribute_category             VARCHAR2(30),
  attribute1                     VARCHAR2(150),
  attribute2                     VARCHAR2(150),
  attribute3                     VARCHAR2(150),
  attribute4                     VARCHAR2(150),
  attribute5                     VARCHAR2(150),
  attribute6                     VARCHAR2(150),
  attribute7                     VARCHAR2(150),
  attribute8                     VARCHAR2(150),
  attribute9                     VARCHAR2(150),
  attribute10                    VARCHAR2(150),
  request_id                     NUMBER(15),
  program_application_id         NUMBER(15),
  program_id                     NUMBER(15),
  program_update_date            DATE,
  rule_start_date                DATE,
  initial_customer_trx_line_id   NUMBER(15),
  interface_line_context         VARCHAR2(30),
  interface_line_attribute1      VARCHAR2(150),
  interface_line_attribute2      VARCHAR2(150),
  interface_line_attribute3      VARCHAR2(150),
  interface_line_attribute4      VARCHAR2(150),
  interface_line_attribute5      VARCHAR2(150),
  interface_line_attribute6      VARCHAR2(150),
  interface_line_attribute7      VARCHAR2(150),
  interface_line_attribute8      VARCHAR2(150),
  sales_order_source             VARCHAR2(50),
  taxable_flag                   VARCHAR2(1),
  extended_amount                NUMBER not null,
  revenue_amount                 NUMBER,
  autorule_complete_flag         VARCHAR2(1),
  link_to_cust_trx_line_id       NUMBER(15),
  attribute11                    VARCHAR2(150),
  attribute12                    VARCHAR2(150),
  attribute13                    VARCHAR2(150),
  attribute14                    VARCHAR2(150),
  attribute15                    VARCHAR2(150),
  tax_precedence                 NUMBER,
  tax_rate                       NUMBER,
  item_exception_rate_id         NUMBER(15),
  tax_exemption_id               NUMBER(15),
  memo_line_id                   NUMBER(15),
  autorule_duration_processed    NUMBER(15),
  uom_code                       VARCHAR2(3),
  default_ussgl_transaction_code VARCHAR2(30),
  default_ussgl_trx_code_context VARCHAR2(30),
  interface_line_attribute10     VARCHAR2(150),
  interface_line_attribute11     VARCHAR2(150),
  interface_line_attribute12     VARCHAR2(150),
  interface_line_attribute13     VARCHAR2(150),
  interface_line_attribute14     VARCHAR2(150),
  interface_line_attribute15     VARCHAR2(150),
  interface_line_attribute9      VARCHAR2(150),
  vat_tax_id                     NUMBER(15),
  autotax                        VARCHAR2(1),
  last_period_to_credit          NUMBER,
  item_context                   VARCHAR2(30),
  tax_exempt_flag                VARCHAR2(1),
  tax_exempt_number              VARCHAR2(80),
  tax_exempt_reason_code         VARCHAR2(30),
  tax_vendor_return_code         VARCHAR2(30),
  sales_tax_id                   NUMBER(15),
  location_segment_id            NUMBER(15),
  movement_id                    NUMBER(15),
  org_id                         NUMBER(15),
  wh_update_date                 DATE,
  global_attribute1              VARCHAR2(150),
  global_attribute2              VARCHAR2(150),
  global_attribute3              VARCHAR2(150),
  global_attribute4              VARCHAR2(150),
  global_attribute5              VARCHAR2(150),
  global_attribute6              VARCHAR2(150),
  global_attribute7              VARCHAR2(150),
  global_attribute8              VARCHAR2(150),
  global_attribute9              VARCHAR2(150),
  global_attribute10             VARCHAR2(150),
  global_attribute11             VARCHAR2(150),
  global_attribute12             VARCHAR2(150),
  global_attribute13             VARCHAR2(150),
  global_attribute14             VARCHAR2(150),
  global_attribute15             VARCHAR2(150),
  global_attribute16             VARCHAR2(150),
  global_attribute17             VARCHAR2(150),
  global_attribute18             VARCHAR2(150),
  global_attribute19             VARCHAR2(150),
  global_attribute20             VARCHAR2(150),
  global_attribute_category      VARCHAR2(30),
  gross_unit_selling_price       NUMBER,
  gross_extended_amount          NUMBER,
  amount_includes_tax_flag       VARCHAR2(1),
  taxable_amount                 NUMBER,
  warehouse_id                   NUMBER(15),
  translated_description         VARCHAR2(1000),
  extended_acctd_amount          NUMBER,
  br_ref_customer_trx_id         NUMBER(15),
  br_ref_payment_schedule_id     NUMBER(15),
  br_adjustment_id               NUMBER(15),
  mrc_extended_acctd_amount      VARCHAR2(2000),
  payment_set_id                 NUMBER(15),
  contract_line_id               NUMBER,
  source_data_key1               VARCHAR2(150),
  source_data_key2               VARCHAR2(150),
  source_data_key3               VARCHAR2(150),
  source_data_key4               VARCHAR2(150),
  source_data_key5               VARCHAR2(150),
  invoiced_line_acctg_level      VARCHAR2(15),
  override_auto_accounting_flag  VARCHAR2(1),
  ship_to_customer_id            NUMBER(15),
  ship_to_address_id             NUMBER(15),
  ship_to_site_use_id            NUMBER(15),
  ship_to_contact_id             NUMBER(15),
  historical_flag                VARCHAR2(1),
  tax_line_id                    NUMBER(15),
  line_recoverable               NUMBER,
  tax_recoverable                NUMBER,
  tax_classification_code        VARCHAR2(30),
  amount_due_remaining           NUMBER,
  acctd_amount_due_remaining     NUMBER,
  amount_due_original            NUMBER,
  acctd_amount_due_original      NUMBER,
  chrg_amount_remaining          NUMBER,
  chrg_acctd_amount_remaining    NUMBER,
  frt_adj_remaining              NUMBER,
  frt_adj_acctd_remaining        NUMBER,
  frt_ed_amount                  NUMBER,
  frt_ed_acctd_amount            NUMBER,
  frt_uned_amount                NUMBER,
  frt_uned_acctd_amount          NUMBER,
  deferral_exclusion_flag        VARCHAR2(1),
  rule_end_date                  DATE,
  payment_trxn_extension_id      NUMBER(15),
  interest_line_id               NUMBER(15),
  ods_creation_date              DATE
)


-- Create table
create table NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01
(
  cust_trx_line_gl_dist_id       NUMBER(15) not null,
  customer_trx_line_id           NUMBER(15),
  code_combination_id            NUMBER(15) not null,
  set_of_books_id                NUMBER(15) not null,
  last_update_date               DATE not null,
  last_updated_by                NUMBER(15) not null,
  creation_date                  DATE not null,
  created_by                     NUMBER(15) not null,
  last_update_login              NUMBER(15),
  percent                        NUMBER,
  amount                         NUMBER,
  gl_date                        DATE,
  gl_posted_date                 DATE,
  cust_trx_line_salesrep_id      NUMBER(15),
  comments                       VARCHAR2(240),
  attribute_category             VARCHAR2(30),
  attribute1                     VARCHAR2(150),
  attribute2                     VARCHAR2(150),
  attribute3                     VARCHAR2(150),
  attribute4                     VARCHAR2(150),
  attribute5                     VARCHAR2(150),
  attribute6                     VARCHAR2(150),
  attribute7                     VARCHAR2(150),
  attribute8                     VARCHAR2(150),
  attribute9                     VARCHAR2(150),
  attribute10                    VARCHAR2(150),
  request_id                     NUMBER(15),
  program_application_id         NUMBER(15),
  program_id                     NUMBER(15),
  program_update_date            DATE,
  concatenated_segments          VARCHAR2(240),
  original_gl_date               DATE,
  post_request_id                NUMBER(15),
  posting_control_id             NUMBER(15) not null,
  account_class                  VARCHAR2(20) not null,
  ra_post_loop_number            NUMBER(15),
  customer_trx_id                NUMBER(15) not null,
  account_set_flag               VARCHAR2(1) not null,
  acctd_amount                   NUMBER,
  ussgl_transaction_code         VARCHAR2(30),
  ussgl_transaction_code_context VARCHAR2(30),
  attribute11                    VARCHAR2(150),
  attribute12                    VARCHAR2(150),
  attribute13                    VARCHAR2(150),
  attribute14                    VARCHAR2(150),
  attribute15                    VARCHAR2(150),
  latest_rec_flag                VARCHAR2(1),
  org_id                         NUMBER(15),
  mrc_account_class              VARCHAR2(2000),
  mrc_customer_trx_id            VARCHAR2(2000),
  mrc_amount                     VARCHAR2(2000),
  mrc_gl_posted_date             VARCHAR2(2000),
  mrc_posting_control_id         VARCHAR2(2000),
  mrc_acctd_amount               VARCHAR2(2000),
  collected_tax_ccid             NUMBER(15),
  collected_tax_concat_seg       VARCHAR2(240),
  revenue_adjustment_id          NUMBER(15),
  rev_adj_class_temp             VARCHAR2(30),
  rec_offset_flag                VARCHAR2(1),
  event_id                       NUMBER(15),
  user_generated_flag            VARCHAR2(1),
  rounding_correction_flag       VARCHAR2(1),
  cogs_request_id                NUMBER(15),
  ods_creation_date              DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate indexes 
create index NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01_N1 on NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01 (LAST_UPDATE_DATE)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01_N2 on NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01 (ORG_ID, CREATED_BY, LAST_UPDATE_DATE)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create unique index NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01_U1 on NODS_ERP.RA_CUST_TRX_LINE_GL_DIST_01 (CUST_TRX_LINE_GL_DIST_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create table
create table NODS_ERP.AR_CASH_RECEIPTS_ALL_01
(
  cash_receipt_id                NUMBER(15) not null,
  last_updated_by                NUMBER(15) not null,
  last_update_date               DATE not null,
  last_update_login              NUMBER(15),
  created_by                     NUMBER(15) not null,
  creation_date                  DATE not null,
  amount                         NUMBER not null,
  set_of_books_id                NUMBER(15) not null,
  currency_code                  VARCHAR2(15) not null,
  receivables_trx_id             NUMBER(15),
  pay_from_customer              NUMBER(15),
  status                         VARCHAR2(30),
  type                           VARCHAR2(20),
  receipt_number                 VARCHAR2(30),
  receipt_date                   DATE not null,
  misc_payment_source            VARCHAR2(30),
  comments                       VARCHAR2(2000),
  distribution_set_id            NUMBER(15),
  reversal_date                  DATE,
  reversal_category              VARCHAR2(20),
  reversal_reason_code           VARCHAR2(30),
  reversal_comments              VARCHAR2(240),
  exchange_rate_type             VARCHAR2(30),
  exchange_rate                  NUMBER,
  exchange_date                  DATE,
  attribute_category             VARCHAR2(30),
  attribute1                     VARCHAR2(150),
  attribute2                     VARCHAR2(150),
  attribute3                     VARCHAR2(150),
  attribute4                     VARCHAR2(150),
  attribute5                     VARCHAR2(150),
  attribute6                     VARCHAR2(150),
  attribute7                     VARCHAR2(150),
  attribute8                     VARCHAR2(150),
  attribute9                     VARCHAR2(150),
  attribute10                    VARCHAR2(150),
  remittance_bank_account_id     NUMBER(15),
  attribute11                    VARCHAR2(150),
  attribute12                    VARCHAR2(150),
  attribute13                    VARCHAR2(150),
  attribute14                    VARCHAR2(150),
  attribute15                    VARCHAR2(150),
  confirmed_flag                 VARCHAR2(1),
  customer_bank_account_id       NUMBER(15),
  customer_site_use_id           NUMBER(15),
  deposit_date                   DATE,
  program_application_id         NUMBER(15),
  program_id                     NUMBER(15),
  program_update_date            DATE,
  receipt_method_id              NUMBER(15) not null,
  request_id                     NUMBER(15),
  selected_for_factoring_flag    VARCHAR2(1),
  selected_remittance_batch_id   NUMBER(15),
  factor_discount_amount         NUMBER,
  ussgl_transaction_code         VARCHAR2(30),
  ussgl_transaction_code_context VARCHAR2(30),
  doc_sequence_value             NUMBER(15),
  doc_sequence_id                NUMBER(15),
  vat_tax_id                     NUMBER(15),
  reference_type                 VARCHAR2(30),
  reference_id                   NUMBER(15),
  customer_receipt_reference     VARCHAR2(30),
  override_remit_account_flag    VARCHAR2(1),
  org_id                         NUMBER(15),
  anticipated_clearing_date      DATE,
  global_attribute1              VARCHAR2(150),
  global_attribute2              VARCHAR2(150),
  global_attribute3              VARCHAR2(150),
  global_attribute4              VARCHAR2(150),
  global_attribute5              VARCHAR2(150),
  global_attribute6              VARCHAR2(150),
  global_attribute7              VARCHAR2(150),
  global_attribute8              VARCHAR2(150),
  global_attribute9              VARCHAR2(150),
  global_attribute10             VARCHAR2(150),
  global_attribute11             VARCHAR2(150),
  global_attribute12             VARCHAR2(150),
  global_attribute13             VARCHAR2(150),
  global_attribute14             VARCHAR2(150),
  global_attribute15             VARCHAR2(150),
  global_attribute16             VARCHAR2(150),
  global_attribute17             VARCHAR2(150),
  global_attribute18             VARCHAR2(150),
  global_attribute19             VARCHAR2(150),
  global_attribute20             VARCHAR2(150),
  global_attribute_category      VARCHAR2(30),
  issuer_name                    VARCHAR2(50),
  issue_date                     DATE,
  issuer_bank_branch_id          NUMBER(15),
  customer_bank_branch_id        NUMBER(15),
  mrc_exchange_rate_type         VARCHAR2(2000),
  mrc_exchange_rate              VARCHAR2(2000),
  mrc_exchange_date              VARCHAR2(2000),
  payment_server_order_num       VARCHAR2(80),
  approval_code                  VARCHAR2(80),
  address_verification_code      VARCHAR2(80),
  tax_rate                       NUMBER,
  actual_value_date              DATE,
  postmark_date                  DATE,
  application_notes              VARCHAR2(2000),
  unique_reference               VARCHAR2(32),
  promise_source                 VARCHAR2(30),
  rec_version_number             NUMBER(15),
  cc_error_code                  VARCHAR2(80),
  cc_error_text                  VARCHAR2(255),
  cc_error_flag                  VARCHAR2(1),
  remit_bank_acct_use_id         NUMBER(15),
  old_customer_bank_branch_id    NUMBER(15),
  old_issuer_bank_branch_id      NUMBER(15),
  legal_entity_id                NUMBER(15),
  payment_trxn_extension_id      NUMBER(15),
  ax_accounted_flag              VARCHAR2(1),
  old_customer_bank_account_id   NUMBER(15),
  cash_appln_owner_id            NUMBER(15),
  work_item_assignment_date      DATE,
  work_item_review_date          DATE,
  work_item_status_code          VARCHAR2(30),
  work_item_review_note          VARCHAR2(2000),
  prev_pay_from_customer         NUMBER(15),
  prev_customer_site_use_id      NUMBER(15),
  work_item_exception_reason     VARCHAR2(30),
  automatch_set_id               NUMBER(15),
  autoapply_flag                 VARCHAR2(1),
  ods_creation_date              DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate indexes 
create index NODS_ERP.AR_CASH_RECEIPTS_ALL_01_N1 on NODS_ERP.AR_CASH_RECEIPTS_ALL_01 (ORG_ID, RECEIPT_METHOD_ID, REMIT_BANK_ACCT_USE_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.AR_CASH_RECEIPTS_ALL_01_N2 on NODS_ERP.AR_CASH_RECEIPTS_ALL_01 (LAST_UPDATE_DATE)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.AR_CASH_RECEIPTS_ALL_01_N3 on NODS_ERP.AR_CASH_RECEIPTS_ALL_01 (ORG_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create unique index NODS_ERP.AR_CASH_RECEIPTS_ALL_01_U1 on NODS_ERP.AR_CASH_RECEIPTS_ALL_01 (CASH_RECEIPT_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );


-- Create table
create table NODS_ERP.AR_CASH_RECEIPT_HISTORY_ALL_01
(
  cash_receipt_history_id       NUMBER(15) not null,
  cash_receipt_id               NUMBER(15) not null,
  status                        VARCHAR2(30) not null,
  trx_date                      DATE not null,
  amount                        NUMBER not null,
  first_posted_record_flag      VARCHAR2(1) not null,
  postable_flag                 VARCHAR2(1) not null,
  factor_flag                   VARCHAR2(1) not null,
  gl_date                       DATE not null,
  current_record_flag           VARCHAR2(1),
  batch_id                      NUMBER(15),
  account_code_combination_id   NUMBER(15),
  reversal_gl_date              DATE,
  reversal_cash_receipt_hist_id NUMBER(15),
  factor_discount_amount        NUMBER,
  bank_charge_account_ccid      NUMBER(15),
  posting_control_id            NUMBER(15) not null,
  reversal_posting_control_id   NUMBER(15),
  gl_posted_date                DATE,
  reversal_gl_posted_date       DATE,
  last_update_login             NUMBER(15),
  acctd_amount                  NUMBER not null,
  acctd_factor_discount_amount  NUMBER,
  created_by                    NUMBER(15) not null,
  creation_date                 DATE not null,
  exchange_date                 DATE,
  exchange_rate                 NUMBER,
  exchange_rate_type            VARCHAR2(30),
  last_update_date              DATE not null,
  program_application_id        NUMBER(15),
  program_id                    NUMBER(15),
  program_update_date           DATE,
  request_id                    NUMBER(15),
  last_updated_by               NUMBER(15) not null,
  prv_stat_cash_receipt_hist_id NUMBER(15),
  created_from                  VARCHAR2(30) not null,
  reversal_created_from         VARCHAR2(30),
  attribute1                    VARCHAR2(150),
  attribute2                    VARCHAR2(150),
  attribute3                    VARCHAR2(150),
  attribute4                    VARCHAR2(150),
  attribute5                    VARCHAR2(150),
  attribute6                    VARCHAR2(150),
  attribute7                    VARCHAR2(150),
  attribute8                    VARCHAR2(150),
  attribute9                    VARCHAR2(150),
  attribute10                   VARCHAR2(150),
  attribute11                   VARCHAR2(150),
  attribute12                   VARCHAR2(150),
  attribute13                   VARCHAR2(150),
  attribute14                   VARCHAR2(150),
  attribute15                   VARCHAR2(150),
  attribute_category            VARCHAR2(30),
  note_status                   VARCHAR2(30),
  org_id                        NUMBER(15),
  mrc_posting_control_id        VARCHAR2(2000),
  mrc_gl_posted_date            VARCHAR2(2000),
  mrc_reversal_gl_posted_date   VARCHAR2(2000),
  mrc_acctd_amount              VARCHAR2(2000),
  mrc_acctd_factor_disc_amount  VARCHAR2(2000),
  mrc_exchange_date             VARCHAR2(2000),
  mrc_exchange_rate             VARCHAR2(2000),
  mrc_exchange_rate_type        VARCHAR2(2000),
  event_id                      NUMBER(15),
  ods_creation_date             DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate indexes 
create index NODS_ERP.AR_CASH_RECEIPT_HIS_ALL_01_N1 on NODS_ERP.AR_CASH_RECEIPT_HISTORY_ALL_01 (LAST_UPDATE_DATE)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.AR_CASH_RECEIPT_HIS_ALL_01_N2 on NODS_ERP.AR_CASH_RECEIPT_HISTORY_ALL_01 (ORG_ID, CREATED_BY, LAST_UPDATED_BY)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create unique index NODS_ERP.AR_CASH_RECEIPT_HIS_ALL_01_U1 on NODS_ERP.AR_CASH_RECEIPT_HISTORY_ALL_01 (CASH_RECEIPT_HISTORY_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create table
create table NODS_ERP.AR_DISTRIBUTIONS_ALL_01
(
  line_id                      NUMBER(15) not null,
  source_id                    NUMBER(15) not null,
  source_table                 VARCHAR2(10) not null,
  source_type                  VARCHAR2(30) not null,
  code_combination_id          NUMBER(15) not null,
  amount_dr                    NUMBER,
  amount_cr                    NUMBER,
  acctd_amount_dr              NUMBER,
  acctd_amount_cr              NUMBER,
  creation_date                DATE not null,
  created_by                   NUMBER(15) not null,
  last_updated_by              NUMBER(15) not null,
  last_update_date             DATE not null,
  last_update_login            NUMBER(15),
  org_id                       NUMBER(15),
  source_table_secondary       VARCHAR2(10),
  source_id_secondary          NUMBER(15),
  currency_code                VARCHAR2(15),
  currency_conversion_rate     NUMBER,
  currency_conversion_type     VARCHAR2(30),
  currency_conversion_date     DATE,
  taxable_entered_dr           NUMBER,
  taxable_entered_cr           NUMBER,
  taxable_accounted_dr         NUMBER,
  taxable_accounted_cr         NUMBER,
  tax_link_id                  NUMBER(15),
  third_party_id               NUMBER(15),
  third_party_sub_id           NUMBER(15),
  reversed_source_id           NUMBER(15),
  tax_code_id                  NUMBER(15),
  location_segment_id          NUMBER(15),
  source_type_secondary        VARCHAR2(30),
  tax_group_code_id            NUMBER(15),
  ref_customer_trx_line_id     NUMBER(15),
  ref_cust_trx_line_gl_dist_id NUMBER(15),
  ref_account_class            VARCHAR2(30),
  activity_bucket              VARCHAR2(30),
  ref_line_id                  NUMBER(15),
  from_amount_dr               NUMBER,
  from_amount_cr               NUMBER,
  from_acctd_amount_dr         NUMBER,
  from_acctd_amount_cr         NUMBER,
  ref_mf_dist_flag             VARCHAR2(1),
  ref_dist_ccid                NUMBER(15),
  ods_creation_date            DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate indexes 
create index NODS_ERP.AR_DISTRIBUTIONS_ALL_01_N1 on NODS_ERP.AR_DISTRIBUTIONS_ALL_01 (LAST_UPDATE_DATE)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.AR_DISTRIBUTIONS_ALL_01_N2 on NODS_ERP.AR_DISTRIBUTIONS_ALL_01 (SOURCE_TABLE)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.AR_DISTRIBUTIONS_ALL_01_N3 on NODS_ERP.AR_DISTRIBUTIONS_ALL_01 (CODE_COMBINATION_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create unique index NODS_ERP.AR_DISTRIBUTIONS_ALL_01_U1 on NODS_ERP.AR_DISTRIBUTIONS_ALL_01 (LINE_ID)
  tablespace NODS_INDEX
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create table
create table NODS_ERP.GL_JE_SOURCES_TL_01
(
  je_source_name           VARCHAR2(25) not null,
  language                 VARCHAR2(4) not null,
  source_lang              VARCHAR2(4) not null,
  last_update_date         DATE not null,
  last_updated_by          NUMBER(15) not null,
  override_edits_flag      VARCHAR2(1) not null,
  user_je_source_name      VARCHAR2(25) not null,
  journal_reference_flag   VARCHAR2(1) not null,
  journal_approval_flag    VARCHAR2(1) not null,
  effective_date_rule_code VARCHAR2(1) not null,
  creation_date            DATE,
  created_by               NUMBER(15),
  last_update_login        NUMBER(15),
  description              VARCHAR2(240),
  attribute1               VARCHAR2(150),
  attribute2               VARCHAR2(150),
  attribute3               VARCHAR2(150),
  attribute4               VARCHAR2(150),
  attribute5               VARCHAR2(150),
  context                  VARCHAR2(150),
  import_using_key_flag    VARCHAR2(1) not null,
  je_source_key            VARCHAR2(25) not null,
  ods_creation_date        DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create table
create table NODS_ERP.HZ_CUST_ACCOUNTS_01
(
  cust_account_id                NUMBER(15) not null,
  party_id                       NUMBER(15) not null,
  last_update_date               DATE not null,
  account_number                 VARCHAR2(30) not null,
  last_updated_by                NUMBER(15) not null,
  creation_date                  DATE not null,
  created_by                     NUMBER(15) not null,
  last_update_login              NUMBER(15),
  request_id                     NUMBER(15),
  program_application_id         NUMBER(15),
  program_id                     NUMBER(15),
  program_update_date            DATE,
  wh_update_date                 DATE,
  attribute_category             VARCHAR2(30),
  attribute1                     VARCHAR2(150),
  attribute2                     VARCHAR2(150),
  attribute3                     VARCHAR2(150),
  attribute4                     VARCHAR2(150),
  attribute5                     VARCHAR2(150),
  attribute6                     VARCHAR2(150),
  attribute7                     VARCHAR2(150),
  attribute8                     VARCHAR2(150),
  attribute9                     VARCHAR2(150),
  attribute10                    VARCHAR2(150),
  attribute11                    VARCHAR2(150),
  attribute12                    VARCHAR2(150),
  attribute13                    VARCHAR2(150),
  attribute14                    VARCHAR2(150),
  attribute15                    VARCHAR2(150),
  attribute16                    VARCHAR2(150),
  attribute17                    VARCHAR2(150),
  attribute18                    VARCHAR2(150),
  attribute19                    VARCHAR2(150),
  attribute20                    VARCHAR2(150),
  global_attribute_category      VARCHAR2(30),
  global_attribute1              VARCHAR2(150),
  global_attribute2              VARCHAR2(150),
  global_attribute3              VARCHAR2(150),
  global_attribute4              VARCHAR2(150),
  global_attribute5              VARCHAR2(150),
  global_attribute6              VARCHAR2(150),
  global_attribute7              VARCHAR2(150),
  global_attribute8              VARCHAR2(150),
  global_attribute9              VARCHAR2(150),
  global_attribute10             VARCHAR2(150),
  global_attribute11             VARCHAR2(150),
  global_attribute12             VARCHAR2(150),
  global_attribute13             VARCHAR2(150),
  global_attribute14             VARCHAR2(150),
  global_attribute15             VARCHAR2(150),
  global_attribute16             VARCHAR2(150),
  global_attribute17             VARCHAR2(150),
  global_attribute18             VARCHAR2(150),
  global_attribute19             VARCHAR2(150),
  global_attribute20             VARCHAR2(150),
  orig_system_reference          VARCHAR2(240) not null,
  status                         VARCHAR2(1) not null,
  customer_type                  VARCHAR2(30),
  customer_class_code            VARCHAR2(30),
  primary_salesrep_id            NUMBER(15),
  sales_channel_code             VARCHAR2(30),
  order_type_id                  NUMBER(15),
  price_list_id                  NUMBER(15),
  subcategory_code               VARCHAR2(30),
  tax_code                       VARCHAR2(50),
  fob_point                      VARCHAR2(30),
  freight_term                   VARCHAR2(30),
  ship_partial                   VARCHAR2(1),
  ship_via                       VARCHAR2(30),
  warehouse_id                   NUMBER(15),
  payment_term_id                NUMBER(15),
  tax_header_level_flag          VARCHAR2(1),
  tax_rounding_rule              VARCHAR2(30),
  coterminate_day_month          VARCHAR2(6),
  primary_specialist_id          NUMBER(15),
  secondary_specialist_id        NUMBER(15),
  account_liable_flag            VARCHAR2(1),
  restriction_limit_amount       NUMBER,
  current_balance                NUMBER,
  password_text                  VARCHAR2(60),
  high_priority_indicator        VARCHAR2(1),
  account_established_date       DATE,
  account_termination_date       DATE,
  account_activation_date        DATE,
  credit_classification_code     VARCHAR2(30),
  department                     VARCHAR2(30),
  major_account_number           VARCHAR2(30),
  hotwatch_service_flag          VARCHAR2(1),
  hotwatch_svc_bal_ind           VARCHAR2(30),
  held_bill_expiration_date      DATE,
  hold_bill_flag                 VARCHAR2(1),
  high_priority_remarks          VARCHAR2(80),
  po_effective_date              DATE,
  po_expiration_date             DATE,
  realtime_rate_flag             VARCHAR2(1),
  single_user_flag               VARCHAR2(1),
  watch_account_flag             VARCHAR2(1),
  watch_balance_indicator        VARCHAR2(1),
  geo_code                       VARCHAR2(30),
  acct_life_cycle_status         VARCHAR2(30),
  account_name                   VARCHAR2(240),
  deposit_refund_method          VARCHAR2(20),
  dormant_account_flag           VARCHAR2(1),
  npa_number                     VARCHAR2(60),
  pin_number                     NUMBER(16),
  suspension_date                DATE,
  write_off_adjustment_amount    NUMBER,
  write_off_payment_amount       NUMBER,
  write_off_amount               NUMBER,
  source_code                    VARCHAR2(150),
  competitor_type                VARCHAR2(150),
  comments                       VARCHAR2(240),
  dates_negative_tolerance       NUMBER,
  dates_positive_tolerance       NUMBER,
  date_type_preference           VARCHAR2(20),
  over_shipment_tolerance        NUMBER,
  under_shipment_tolerance       NUMBER,
  over_return_tolerance          NUMBER,
  under_return_tolerance         NUMBER,
  item_cross_ref_pref            VARCHAR2(30),
  ship_sets_include_lines_flag   VARCHAR2(1),
  arrivalsets_include_lines_flag VARCHAR2(1),
  sched_date_push_flag           VARCHAR2(1),
  invoice_quantity_rule          VARCHAR2(30),
  pricing_event                  VARCHAR2(30),
  account_replication_key        NUMBER(15),
  status_update_date             DATE,
  autopay_flag                   VARCHAR2(1),
  notify_flag                    VARCHAR2(1),
  last_batch_id                  NUMBER,
  org_id                         NUMBER(15),
  object_version_number          NUMBER,
  created_by_module              VARCHAR2(150),
  application_id                 NUMBER,
  selling_party_id               NUMBER(15),
  ods_creation_date              DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create table
create table NODS_ERP.HZ_PARTIES_01
(
  party_id                    NUMBER(15) not null,
  party_number                VARCHAR2(30) not null,
  party_name                  VARCHAR2(360) not null,
  party_type                  VARCHAR2(30) not null,
  validated_flag              VARCHAR2(1),
  last_updated_by             NUMBER(15) not null,
  creation_date               DATE not null,
  last_update_login           NUMBER(15),
  request_id                  NUMBER(15),
  program_application_id      NUMBER(15),
  created_by                  NUMBER(15) not null,
  last_update_date            DATE not null,
  program_id                  NUMBER(15),
  program_update_date         DATE,
  wh_update_date              DATE,
  attribute_category          VARCHAR2(30),
  attribute1                  VARCHAR2(150),
  attribute2                  VARCHAR2(150),
  attribute3                  VARCHAR2(150),
  attribute4                  VARCHAR2(150),
  attribute5                  VARCHAR2(150),
  attribute6                  VARCHAR2(150),
  attribute7                  VARCHAR2(150),
  attribute8                  VARCHAR2(150),
  attribute9                  VARCHAR2(150),
  attribute10                 VARCHAR2(150),
  attribute11                 VARCHAR2(150),
  attribute12                 VARCHAR2(150),
  attribute13                 VARCHAR2(150),
  attribute14                 VARCHAR2(150),
  attribute15                 VARCHAR2(150),
  attribute16                 VARCHAR2(150),
  attribute17                 VARCHAR2(150),
  attribute18                 VARCHAR2(150),
  attribute19                 VARCHAR2(150),
  attribute20                 VARCHAR2(150),
  attribute21                 VARCHAR2(150),
  attribute22                 VARCHAR2(150),
  attribute23                 VARCHAR2(150),
  attribute24                 VARCHAR2(150),
  global_attribute_category   VARCHAR2(30),
  global_attribute1           VARCHAR2(150),
  global_attribute2           VARCHAR2(150),
  global_attribute4           VARCHAR2(150),
  global_attribute3           VARCHAR2(150),
  global_attribute5           VARCHAR2(150),
  global_attribute6           VARCHAR2(150),
  global_attribute7           VARCHAR2(150),
  global_attribute8           VARCHAR2(150),
  global_attribute9           VARCHAR2(150),
  global_attribute10          VARCHAR2(150),
  global_attribute11          VARCHAR2(150),
  global_attribute12          VARCHAR2(150),
  global_attribute13          VARCHAR2(150),
  global_attribute14          VARCHAR2(150),
  global_attribute15          VARCHAR2(150),
  global_attribute16          VARCHAR2(150),
  global_attribute17          VARCHAR2(150),
  global_attribute18          VARCHAR2(150),
  global_attribute19          VARCHAR2(150),
  global_attribute20          VARCHAR2(150),
  orig_system_reference       VARCHAR2(240) not null,
  sic_code                    VARCHAR2(30),
  hq_branch_ind               VARCHAR2(30),
  customer_key                VARCHAR2(500),
  tax_reference               VARCHAR2(50),
  jgzz_fiscal_code            VARCHAR2(20),
  duns_number                 NUMBER,
  tax_name                    VARCHAR2(60),
  person_pre_name_adjunct     VARCHAR2(30),
  person_first_name           VARCHAR2(150),
  person_middle_name          VARCHAR2(60),
  person_last_name            VARCHAR2(150),
  person_name_suffix          VARCHAR2(30),
  person_title                VARCHAR2(60),
  person_academic_title       VARCHAR2(30),
  person_previous_last_name   VARCHAR2(150),
  known_as                    VARCHAR2(240),
  person_iden_type            VARCHAR2(30),
  person_identifier           VARCHAR2(60),
  group_type                  VARCHAR2(30),
  country                     VARCHAR2(60),
  address1                    VARCHAR2(240),
  address2                    VARCHAR2(240),
  address3                    VARCHAR2(240),
  address4                    VARCHAR2(240),
  city                        VARCHAR2(60),
  postal_code                 VARCHAR2(60),
  state                       VARCHAR2(60),
  province                    VARCHAR2(60),
  status                      VARCHAR2(1) not null,
  county                      VARCHAR2(60),
  sic_code_type               VARCHAR2(30),
  total_num_of_orders         NUMBER,
  total_ordered_amount        NUMBER,
  last_ordered_date           DATE,
  url                         VARCHAR2(2000),
  email_address               VARCHAR2(2000),
  do_not_mail_flag            VARCHAR2(1),
  analysis_fy                 VARCHAR2(5),
  fiscal_yearend_month        VARCHAR2(30),
  employees_total             NUMBER,
  curr_fy_potential_revenue   NUMBER,
  next_fy_potential_revenue   NUMBER,
  year_established            NUMBER(4),
  gsa_indicator_flag          VARCHAR2(1),
  mission_statement           VARCHAR2(2000),
  organization_name_phonetic  VARCHAR2(320),
  person_first_name_phonetic  VARCHAR2(60),
  person_last_name_phonetic   VARCHAR2(60),
  language_name               VARCHAR2(4),
  category_code               VARCHAR2(30),
  reference_use_flag          VARCHAR2(1),
  third_party_flag            VARCHAR2(1),
  competitor_flag             VARCHAR2(1),
  salutation                  VARCHAR2(60),
  known_as2                   VARCHAR2(240),
  known_as3                   VARCHAR2(240),
  known_as4                   VARCHAR2(240),
  known_as5                   VARCHAR2(240),
  duns_number_c               VARCHAR2(30),
  object_version_number       NUMBER,
  created_by_module           VARCHAR2(150),
  application_id              NUMBER,
  primary_phone_contact_pt_id NUMBER(15),
  primary_phone_purpose       VARCHAR2(30),
  primary_phone_line_type     VARCHAR2(30),
  primary_phone_country_code  VARCHAR2(10),
  primary_phone_area_code     VARCHAR2(10),
  primary_phone_number        VARCHAR2(40),
  primary_phone_extension     VARCHAR2(20),
  certification_level         VARCHAR2(30),
  cert_reason_code            VARCHAR2(30),
  preferred_contact_method    VARCHAR2(30),
  home_country                VARCHAR2(2),
  person_bo_version           NUMBER(15),
  org_bo_version              NUMBER(15),
  person_cust_bo_version      NUMBER(15),
  org_cust_bo_version         NUMBER(15),
  ods_creation_date           DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create table
create table NODS_ERP.XLA_EVENTS_01
(
  event_id                  NUMBER(15) not null,
  application_id            NUMBER(15) not null,
  event_type_code           VARCHAR2(30) not null,
  event_date                DATE not null,
  entity_id                 NUMBER(15) not null,
  event_status_code         VARCHAR2(1) not null,
  process_status_code       VARCHAR2(1) not null,
  reference_num_1           NUMBER,
  reference_num_2           NUMBER,
  reference_num_3           NUMBER,
  reference_num_4           NUMBER,
  reference_char_1          VARCHAR2(240),
  reference_char_2          VARCHAR2(240),
  reference_char_3          VARCHAR2(240),
  reference_char_4          VARCHAR2(240),
  reference_date_1          DATE,
  reference_date_2          DATE,
  reference_date_3          DATE,
  reference_date_4          DATE,
  event_number              NUMBER(38) not null,
  on_hold_flag              VARCHAR2(1) not null,
  creation_date             DATE not null,
  created_by                NUMBER(15) not null,
  last_update_date          DATE not null,
  last_updated_by           NUMBER(15) not null,
  last_update_login         NUMBER(15),
  program_update_date       DATE,
  program_application_id    NUMBER(15),
  program_id                NUMBER(15),
  request_id                NUMBER(15),
  upg_batch_id              NUMBER(15),
  upg_source_application_id NUMBER(15),
  upg_valid_flag            VARCHAR2(1),
  transaction_date          DATE not null,
  budgetary_control_flag    VARCHAR2(1),
  merge_event_set_id        NUMBER(15),
  ods_create_date           DATE
)
tablespace NODS_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate indexes 
create index NODS_ERP.XLA_EVENTS_01_1 on NODS_ERP.XLA_EVENTS_01 (EVENT_ID)
  tablespace NODS_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
create index NODS_ERP.XLA_EVENTS_01_2 on NODS_ERP.XLA_EVENTS_01 (LAST_UPDATE_DATE)
  tablespace NODS_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 160K
    next 1M
    minextents 1
    maxextents unlimited
  );
