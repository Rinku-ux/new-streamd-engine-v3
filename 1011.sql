WITH TargetJournals AS (
  SELECT j.id AS journal_id, v.voucher_type_code, j.client_id, j.create_date AS journal_create_date,
         /* 月初〜月末でクリップするため、予め計算しておく */
         STR_TO_DATE(CONCAT('{{ start_date }}', '-01'), '%Y-%c-%d') AS calc_start,
         STR_TO_DATE(CONCAT('{{ end_date }}', '-01'), '%Y-%c-%d') + INTERVAL 1 MONTH AS calc_end
  FROM fa_journal j
  JOIN fa_voucher_detail vd ON j.voucher_detail_id = vd.id
  JOIN fa_voucher v ON vd.voucher_id = v.id
  WHERE j.client_id IN (
    '{{id1}}','{{id2}}','{{id3}}','{{id4}}','{{id5}}','{{id6}}','{{id7}}','{{id8}}','{{id9}}','{{id10}}',
    '{{id11}}','{{id12}}','{{id13}}','{{id14}}','{{id15}}','{{id16}}','{{id17}}','{{id18}}','{{id19}}','{{id20}}',
    '{{id21}}','{{id22}}','{{id23}}','{{id24}}','{{id25}}','{{id26}}','{{id27}}','{{id28}}','{{id29}}','{{id30}}',
    '{{id31}}','{{id32}}','{{id33}}','{{id34}}','{{id35}}','{{id36}}','{{id37}}','{{id38}}','{{id39}}','{{id40}}',
    '{{id41}}','{{id42}}','{{id43}}','{{id44}}','{{id45}}','{{id46}}','{{id47}}','{{id48}}','{{id49}}','{{id50}}'
  )
    AND j.create_date >= STR_TO_DATE(CONCAT('{{ start_date }}', '-01'), '%Y-%c-%d')
    AND j.create_date < STR_TO_DATE(CONCAT('{{ end_date }}', '-01'), '%Y-%c-%d') + INTERVAL 1 MONTH
    
    AND ('{{ voucher_type }}' = 'all' OR FIND_IN_SET(v.voucher_type_code, '{{ voucher_type }}') > 0)
),
TargetHistory AS (
  SELECT th.id, th.journal_id, th.revision, th.operation
  FROM fa_journal_history th
  INNER JOIN TargetJournals tj ON th.journal_id = tj.journal_id
),
FirstManualRev AS (
  SELECT journal_id, MIN(revision) as first_manual_rev
  FROM TargetHistory
  WHERE operation IN ('edit', 'bulk_edit')
  GROUP BY journal_id
),
ValidJournals AS (
  SELECT tj.journal_id, tj.voucher_type_code, tj.client_id, tj.journal_create_date, fmr.first_manual_rev
  FROM TargetJournals tj
  INNER JOIN FirstManualRev fmr ON tj.journal_id = fmr.journal_id
  WHERE NOT EXISTS (
    SELECT 1 FROM TargetHistory th2 
    WHERE th2.journal_id = tj.journal_id 
      AND th2.operation IN ('delete', 'cancel', 'destroy', 'digitalization_cancel')
  )
),
SystemStateRev AS (
  SELECT th.journal_id, MAX(th.revision) as sys_rev
  FROM TargetHistory th
  INNER JOIN ValidJournals vj ON th.journal_id = vj.journal_id
  WHERE th.revision < vj.first_manual_rev
  GROUP BY th.journal_id
),
LatestRevision AS (
  SELECT th.journal_id, MAX(th.revision) AS max_rev
  FROM TargetHistory th
  INNER JOIN ValidJournals vj ON th.journal_id = vj.journal_id
  GROUP BY th.journal_id
),
RequiredHistory AS (
  SELECT th.id, th.journal_id, 'initial' as state_type
  FROM TargetHistory th
  INNER JOIN SystemStateRev sr ON th.journal_id = sr.journal_id AND th.revision = sr.sys_rev
  UNION ALL
  SELECT th.id, th.journal_id, 'latest' as state_type
  FROM TargetHistory th
  INNER JOIN LatestRevision lr ON th.journal_id = lr.journal_id AND th.revision = lr.max_rev
),
TargetDetails AS (
  SELECT 
    fjhd.history_id,
    MAX(fjhd.posting_date) AS posting_date,
    SUM(fjhd.amount) AS total_amount,
    MAX(fjhd.supplier) AS supplier,
    MAX(fjhd.content) AS content,
    MAX(fjhd.invoice_registration_number) AS reg_num,
    GROUP_CONCAT(COALESCE(fjhd.debit_account_id, 0) ORDER BY COALESCE(fjhd.debit_account_id, 0) SEPARATOR ',') AS debit_accounts,
    GROUP_CONCAT(COALESCE(fjhd.debit_tax_type_id, 0) ORDER BY COALESCE(fjhd.debit_account_id, 0) SEPARATOR ',') AS debit_taxes
  FROM fa_journal_history_detail fjhd
  INNER JOIN RequiredHistory rh ON fjhd.history_id = rh.id
  GROUP BY fjhd.history_id
),
InitialState AS (
  SELECT rh.journal_id, td.total_amount, td.debit_accounts, td.posting_date, td.supplier, td.content, td.reg_num, td.debit_taxes
  FROM RequiredHistory rh
  INNER JOIN TargetDetails td ON rh.id = td.history_id
  WHERE rh.state_type = 'initial'
),
LatestState AS (
  SELECT rh.journal_id, td.total_amount, td.debit_accounts, td.posting_date, td.supplier, td.content, td.reg_num, td.debit_taxes
  FROM RequiredHistory rh
  INNER JOIN TargetDetails td ON rh.id = td.history_id
  WHERE rh.state_type = 'latest'
),
BaseDiff AS (
  SELECT 
    vj.client_id,
    DATE_FORMAT(vj.journal_create_date, '%Y-%m') AS target_month,
    vj.journal_id,
    vj.voucher_type_code,
    CAST(CAST(i.total_amount AS SIGNED) AS CHAR) AS i_total_amount,
    CAST(CAST(l.total_amount AS SIGNED) AS CHAR) AS l_total_amount,
    i.posting_date AS raw_posting_date,
    DATE_FORMAT(i.posting_date, '%Y/%m/%d') AS i_posting_date,
    DATE_FORMAT(l.posting_date, '%Y/%m/%d') AS l_posting_date,
    i.supplier AS i_supplier,
    l.supplier AS l_supplier,
    i.debit_accounts AS i_debit_accounts,
    l.debit_accounts AS l_debit_accounts,
    i.debit_taxes AS i_debit_taxes,
    l.debit_taxes AS l_debit_taxes,
    i.reg_num AS i_reg_num,
    l.reg_num AS l_reg_num,
    i.content AS i_content,
    l.content AS l_content
  FROM ValidJournals vj 
  INNER JOIN InitialState i ON vj.journal_id = i.journal_id 
  INNER JOIN LatestState l ON vj.journal_id = l.journal_id
)

SELECT /*+ MAX_EXECUTION_TIME(60000) */
  d.client_id,
  d.target_month,
  d.voucher_type_code,
  d.journal_id,
  d.error_field,
  COALESCE(d.initial_value, '(未入力)') AS initial_value,
  COALESCE(d.latest_value, '(未入力)') AS latest_value
FROM (
  SELECT client_id, target_month, voucher_type_code, journal_id, '金額' AS error_field, i_total_amount AS initial_value, l_total_amount AS latest_value
  FROM BaseDiff WHERE NOT (i_total_amount <=> l_total_amount)
  UNION ALL
  SELECT client_id, target_month, voucher_type_code, journal_id, '日付', i_posting_date, l_posting_date
  FROM BaseDiff WHERE NOT (i_posting_date <=> l_posting_date)
  UNION ALL
  SELECT client_id, target_month, voucher_type_code, journal_id, '支払先', i_supplier, l_supplier
  FROM BaseDiff WHERE NOT (i_supplier <=> l_supplier)
  UNION ALL
  SELECT client_id, target_month, voucher_type_code, journal_id, '科目', i_debit_accounts, l_debit_accounts
  FROM BaseDiff WHERE voucher_type_code NOT IN ('bankbook', 'creditcard', 'totaltransfer', 'medical') 
    AND NOT (i_debit_accounts <=> l_debit_accounts)
  UNION ALL
  SELECT client_id, target_month, voucher_type_code, journal_id, '税区分', i_debit_taxes, l_debit_taxes
  FROM BaseDiff WHERE voucher_type_code IN ('receipt', 'invoice') 
    AND NOT (i_debit_taxes <=> l_debit_taxes)
  UNION ALL
  SELECT client_id, target_month, voucher_type_code, journal_id, '登録番号', i_reg_num, l_reg_num
  FROM BaseDiff WHERE voucher_type_code IN ('receipt', 'invoice') 
    AND NOT (i_reg_num <=> l_reg_num)
  UNION ALL
  SELECT client_id, target_month, voucher_type_code, journal_id, '内容', i_content, l_content
  FROM BaseDiff WHERE voucher_type_code IN ('depositslip', 'paymentslip') 
    AND NOT (i_content <=> l_content)
) d
WHERE 
  '{{item_filter}}' = 'overall'
  OR ('{{item_filter}}' = 'amount' AND d.error_field = '金額')
  OR ('{{item_filter}}' = 'date' AND d.error_field = '日付')
  OR ('{{item_filter}}' = 'account' AND d.error_field = '科目')
  OR ('{{item_filter}}' = 'supplier' AND d.error_field = '支払先')
  OR ('{{item_filter}}' = 'tax' AND d.error_field = '税区分')
  OR ('{{item_filter}}' = 'regnum' AND d.error_field = '登録番号')
  OR ('{{item_filter}}' = 'content' AND d.error_field = '内容');