WITH 
TargetHistory AS (
  SELECT client_id, id, journal_id, revision, operation
  FROM fa_journal_history
  WHERE client_id IN (
    '{{id1}}', '{{id2}}', '{{id3}}', '{{id4}}', '{{id5}}', 
    '{{id6}}', '{{id7}}', '{{id8}}', '{{id9}}', '{{id10}}',
    '{{id11}}', '{{id12}}', '{{id13}}', '{{id14}}', '{{id15}}',
    '{{id16}}', '{{id17}}', '{{id18}}', '{{id19}}', '{{id20}}',
    '{{id21}}', '{{id22}}', '{{id23}}', '{{id24}}', '{{id25}}',
    '{{id26}}', '{{id27}}', '{{id28}}', '{{id29}}', '{{id30}}',
    '{{id31}}', '{{id32}}', '{{id33}}', '{{id34}}', '{{id35}}',
    '{{id36}}', '{{id37}}', '{{id38}}', '{{id39}}', '{{id40}}',
    '{{id41}}', '{{id42}}', '{{id43}}', '{{id44}}', '{{id45}}',
    '{{id46}}', '{{id47}}', '{{id48}}', '{{id49}}', '{{id50}}'
  )
  AND create_date >= CURDATE() - INTERVAL 37 MONTH
),
TargetJournals AS (
  SELECT 
    h.journal_id,
    MAX(j.create_date) AS journal_create_date 
  FROM TargetHistory h
  JOIN fa_journal j ON h.journal_id = j.id
  WHERE j.create_date >= CURDATE() - INTERVAL 37 MONTH
    -- 削除・キャンセルされた仕訳を除外（元データから復活）
    AND h.journal_id NOT IN (SELECT journal_id FROM TargetHistory WHERE operation IN ('delete', 'cancel', 'destroy', 'digitalization_cancel')) 
  GROUP BY h.journal_id
),
JournalSnapshots AS (
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
  JOIN TargetHistory th ON fjhd.history_id = th.id
  GROUP BY fjhd.history_id
),
FirstManualRev AS (
  SELECT journal_id, MIN(revision) as first_manual_rev
  FROM TargetHistory
  WHERE operation IN ('edit', 'bulk_edit')
  GROUP BY journal_id
),
SystemStateRev AS (
  SELECT th.journal_id, MAX(th.revision) as sys_rev
  FROM TargetHistory th
  LEFT JOIN FirstManualRev fmr ON th.journal_id = fmr.journal_id
  WHERE (fmr.first_manual_rev IS NULL OR th.revision < fmr.first_manual_rev)
  GROUP BY th.journal_id
),
InitialState AS (
  SELECT th.journal_id, js.total_amount, js.debit_accounts, js.posting_date, js.supplier, js.content, js.reg_num, js.debit_taxes
  FROM SystemStateRev sr
  JOIN TargetHistory th ON sr.journal_id = th.journal_id AND sr.sys_rev = th.revision
  JOIN JournalSnapshots js ON th.id = js.history_id
),
LatestRevision AS (
  SELECT journal_id, MAX(revision) AS max_rev
  FROM TargetHistory
  GROUP BY journal_id
),
LatestState AS (
  SELECT th.journal_id, js.total_amount, js.debit_accounts, js.posting_date, js.supplier, js.content, js.reg_num, js.debit_taxes
  FROM TargetHistory th
  JOIN LatestRevision lr ON th.journal_id = lr.journal_id AND th.revision = lr.max_rev
  JOIN JournalSnapshots js ON th.id = js.history_id
),
VoucherInfo AS (
  SELECT 
    j.client_id,
    j.id AS journal_id,
    v.voucher_type_code
  FROM fa_journal j
  JOIN fa_voucher_detail vd ON j.voucher_detail_id = vd.id
  JOIN fa_voucher v ON vd.voucher_id = v.id
  WHERE j.client_id IN (
    '{{id1}}', '{{id2}}', '{{id3}}', '{{id4}}', '{{id5}}', 
    '{{id6}}', '{{id7}}', '{{id8}}', '{{id9}}', '{{id10}}',
    '{{id11}}', '{{id12}}', '{{id13}}', '{{id14}}', '{{id15}}',
    '{{id16}}', '{{id17}}', '{{id18}}', '{{id19}}', '{{id20}}',
    '{{id21}}', '{{id22}}', '{{id23}}', '{{id24}}', '{{id25}}',
    '{{id26}}', '{{id27}}', '{{id28}}', '{{id29}}', '{{id30}}',
    '{{id31}}', '{{id32}}', '{{id33}}', '{{id34}}', '{{id35}}',
    '{{id36}}', '{{id37}}', '{{id38}}', '{{id39}}', '{{id40}}',
    '{{id41}}', '{{id42}}', '{{id43}}', '{{id44}}', '{{id45}}',
    '{{id46}}', '{{id47}}', '{{id48}}', '{{id49}}', '{{id50}}'
  )
  AND v.voucher_type_code NOT IN ('nondigitization', 'online')
),
Comparison AS (
  SELECT 
    v.client_id,
    DATE_FORMAT(t.journal_create_date, '%Y-%m') AS target_month,
    v.voucher_type_code,
    t.journal_id,
    
    CASE WHEN CAST(COALESCE(i.total_amount, 0) AS SIGNED) <=> CAST(COALESCE(l.total_amount, 0) AS SIGNED) THEN 1 ELSE 0 END AS is_amount_correct,
    CASE WHEN DATE_FORMAT(i.posting_date, '%Y/%m/%d') <=> DATE_FORMAT(l.posting_date, '%Y/%m/%d') THEN 1 ELSE 0 END AS is_date_correct,
    CASE WHEN i.supplier <=> l.supplier THEN 1 ELSE 0 END AS is_supplier_correct,
    
    CASE WHEN v.voucher_type_code IN ('bankbook', 'creditcard', 'totaltransfer', 'medical') THEN NULL
         WHEN i.debit_accounts <=> l.debit_accounts THEN 1 ELSE 0 END AS is_account_correct,
         
    CASE WHEN v.voucher_type_code NOT IN ('receipt', 'invoice') THEN NULL
         WHEN i.debit_taxes <=> l.debit_taxes THEN 1 ELSE 0 END AS is_tax_correct,
         
    CASE WHEN v.voucher_type_code NOT IN ('receipt', 'invoice') THEN NULL
         WHEN i.reg_num <=> l.reg_num THEN 1 ELSE 0 END AS is_regnum_correct, 
         
    CASE WHEN v.voucher_type_code NOT IN ('depositslip', 'paymentslip') THEN NULL
         WHEN i.content <=> l.content THEN 1 ELSE 0 END AS is_content_correct, 
         
    CASE 
      WHEN v.voucher_type_code IN ('receipt', 'invoice') THEN
        CASE WHEN (DATE_FORMAT(i.posting_date, '%Y/%m/%d') <=> DATE_FORMAT(l.posting_date, '%Y/%m/%d')) 
              AND (CAST(COALESCE(i.total_amount, 0) AS SIGNED) <=> CAST(COALESCE(l.total_amount, 0) AS SIGNED)) 
              AND (i.debit_accounts <=> l.debit_accounts) AND (i.debit_taxes <=> l.debit_taxes) AND (i.supplier <=> l.supplier) AND (i.reg_num <=> l.reg_num) THEN 1 ELSE 0 END
      WHEN v.voucher_type_code IN ('bankbook', 'creditcard', 'totaltransfer', 'medical') THEN
        CASE WHEN (DATE_FORMAT(i.posting_date, '%Y/%m/%d') <=> DATE_FORMAT(l.posting_date, '%Y/%m/%d')) 
              AND (CAST(COALESCE(i.total_amount, 0) AS SIGNED) <=> CAST(COALESCE(l.total_amount, 0) AS SIGNED)) 
              AND (i.supplier <=> l.supplier) THEN 1 ELSE 0 END
      WHEN v.voucher_type_code IN ('depositslip', 'paymentslip') THEN
        CASE WHEN (DATE_FORMAT(i.posting_date, '%Y/%m/%d') <=> DATE_FORMAT(l.posting_date, '%Y/%m/%d')) 
              AND (CAST(COALESCE(i.total_amount, 0) AS SIGNED) <=> CAST(COALESCE(l.total_amount, 0) AS SIGNED)) 
              AND (i.debit_accounts <=> l.debit_accounts) AND (i.supplier <=> l.supplier) AND (i.content <=> l.content) THEN 1 ELSE 0 END
      ELSE 
        CASE WHEN (DATE_FORMAT(i.posting_date, '%Y/%m/%d') <=> DATE_FORMAT(l.posting_date, '%Y/%m/%d')) 
              AND (CAST(COALESCE(i.total_amount, 0) AS SIGNED) <=> CAST(COALESCE(l.total_amount, 0) AS SIGNED)) 
              AND (i.debit_accounts <=> l.debit_accounts) AND (i.supplier <=> l.supplier) THEN 1 ELSE 0 END
    END AS is_overall_correct
    
  FROM TargetJournals t
  JOIN InitialState i ON t.journal_id = i.journal_id
  JOIN LatestState l ON t.journal_id = l.journal_id
  JOIN VoucherInfo v ON t.journal_id = v.journal_id
  WHERE t.journal_create_date >= CURDATE() - INTERVAL 37 MONTH
    AND t.journal_create_date <= LAST_DAY(CURDATE())
)

SELECT /*+ MAX_EXECUTION_TIME(1800000) */
  client_id,
  target_month,
  voucher_type_code,
  COUNT(is_overall_correct) AS 対象仕訳数,
  SUM(is_overall_correct) AS 全体正解件数,
  COUNT(is_amount_correct) AS 金額_対象,
  SUM(is_amount_correct) AS 金額_正解,
  COUNT(is_date_correct) AS 日付_対象,
  SUM(is_date_correct) AS 日付_正解,
  COUNT(is_account_correct) AS 科目_対象,
  SUM(is_account_correct) AS 科目_正解,
  COUNT(is_supplier_correct) AS 支払先_対象,
  SUM(is_supplier_correct) AS 支払先_正解,
  COUNT(is_tax_correct) AS 税区分_対象,
  SUM(is_tax_correct) AS 税区分_正解,
  COUNT(is_regnum_correct) AS 登録_対象,
  SUM(is_regnum_correct) AS 登録_正解,
  COUNT(is_content_correct) AS 内容_対象,
  SUM(is_content_correct) AS 内容_正解
FROM Comparison
GROUP BY client_id, target_month, voucher_type_code;