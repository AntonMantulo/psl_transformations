{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    unique_key='postingid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "postingcompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}


WITH master AS (
WITH master1 AS (
WITH d AS (
WITH s AS (
SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
      postingcompleted,
      eurexchangerate, 
      payitemname,
      postingtype,
      note
FROM psl.posting AS posting
WHERE (note LIKE 'ReturnAmountCausedByCompletion%' 
  OR note LIKE 'ReleaseBonusWallet%'
  OR note LIKE 'ConfiscateBonusCausedByExpiry%'
  OR note LIKE 'ConfiscateBonusCausedByForfeiture%')
  AND payitemname = 'UBS'
  
ORDER BY 1 
)

SELECT *,
  ROW_NUMBER () OVER (PARTITION BY bonuswalletid) AS rn   
FROM s) 
SELECT * 
FROM d 
WHERE rn = 1

),

bets AS (
SELECT  CAST(TRIM(RIGHT(note, 13)) AS INT64) as bonuswalletid, *
FROM psl.posting
WHERE note like 'DebitBonusWallet%'
),

gamefeed AS(
WITH gamefeed AS
(SELECT gameid, 
  gamegroup,
  productname,
  gamename,
  updated,
  ROW_NUMBER () OVER (PARTITION BY gameid ORDER BY updated DESC) AS rn
FROM `stitch-test-296708.psl.gamefeed` 
ORDER BY 1
)
SELECT *
FROM gamefeed
WHERE rn = 1

), 

gamingtrans AS(
WITH gamingtrans AS
(SELECT *,
  ROW_NUMBER () OVER (PARTITION BY transid) AS rn
FROM `stitch-test-296708.psl.gamingtrans` 
)
SELECT *
FROM gamingtrans
WHERE rn = 1

), 

gamingtrans2 AS (
WITH gamingtrans AS
(SELECT *,
  ROW_NUMBER () OVER (PARTITION BY transid) AS rn
FROM `stitch-test-296708.psl.gamingtrans` 
)
SELECT matchingpostingid, MAX(postingcompleted) as postingcompleted
FROM gamingtrans
JOIN psl.posting
ON gamingtrans.transid = posting.transid
WHERE rn = 1 and matchingpostingid is not null
GROUP BY 1)

SELECT master1.bonuswalletid,
  'BonusMoney' as wallettype,
  bets.userid,
  gamegroup,
  productname,
  gamename,
  bets.postingid,
  master1.postingcompleted,
  bets.amount * bets.eurexchangerate as amounteur,
  bets.amount,
  bets.currency,
  bets.eurexchangerate,
  sessionid
FROM master1
LEFT JOIN bets 
ON master1.bonuswalletid = bets.bonuswalletid
LEFT JOIN gamingtrans
ON bets.transid = gamingtrans.transid
LEFT JOIN gamefeed
ON gamingtrans.gameid = gamefeed.gameid
WHERE postingid is not null

UNION ALL 

SELECT 
  CASE 
    WHEN note like 'DebitRealMoney%' and note != 'DebitRealMoney. BonusWalletID = 0'
    THEN CAST(TRIM(RIGHT(posting.note, 13)) AS INT64)
    ELSE null
   END as bonuswalletid,
  'RealCash' as wallettype,
  posting.userid,
  gamegroup,
  productname,
  gamename,
  posting.postingid,
  CASE 
    WHEN gamename = 'Sports Betting'
    THEN gamingtrans2.postingcompleted
    ELSE posting.postingcompleted
  END as postingcompleted,
  posting.amount * posting.eurexchangerate AS amounteur,
  posting.amount,
  posting.currency,
  posting.eurexchangerate,
  gamingtrans.sessionid
FROM `stitch-test-296708.psl.posting` as posting
LEFT JOIN gamingtrans
ON posting.transid = gamingtrans.transid
LEFT JOIN gamefeed
ON gamingtrans.gameid = gamefeed.gameid
LEFT JOIN gamingtrans2
ON posting.postingid = gamingtrans2.matchingpostingid
WHERE posting.paymenttype = 'Debit' and (posting.note is null or posting.note = '' or posting.note like 'DebitRealMoney%' or posting.note like 'Closed%'))

SELECT *
FROM master 
WHERE postingcompleted is not null

{% if is_incremental() %}
        -- recalculate yesterday + today
        where DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}