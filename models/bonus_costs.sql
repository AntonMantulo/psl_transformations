{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    unique_key='bonuswalletid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "postingcompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}


WITH master AS (
WITH d as (

SELECT 
  CAST(TRIM(RIGHT(posting.note, 13)) AS INT64) as bonuswalletid, 
  CASE
    WHEN note like 'ReleaseBonusWallet%'
    THEN 'released'
    WHEN note like 'ReturnAmountCausedByCompletion%'
    THEN 'used-up'
    WHEN note like 'ConfiscateBonusCausedByExpiry%'
    THEN 'expired'
    WHEN note like 'ConfiscateBonusCausedByForfeiture%'
    THEN 'forfeited'
    ELSE ''
  END as bonus_status, 
  *, 
  ROW_NUMBER () OVER (PARTITION BY CAST(TRIM(RIGHT(posting.note, 13)) AS INT64)) as rn
FROM psl.posting
WHERE (note like 'ReleaseBonusWallet%' and payitemname='UBS')
  OR (note like 'ReturnAmountCausedByCompletion%' and payitemname='UBS')
  OR (note like 'ConfiscateBonusCausedByExpiry%' and payitemname='UBS')
  OR (note like 'ConfiscateBonusCausedByForfeiture%' and payitemname='UBS')
  
  
  
  )

SELECT bonuswalletid, 
  bonus_status,
  postingcompleted,
  amount,
  eurexchangerate,
  userid
FROM D 
WHERE rn = 1),


bonus_granted AS (
  WITH type as (
    SELECT DISTINCT 
      bonuswalletid,
      CASE 
        WHEN gamename = 'Sports Betting'
        THEN 'sport'
        ELSE 'casino'
      END as type
    FROM {{ref ('bets') }}
  )

  SELECT 
    userid,
    CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
    amount * eurexchangerate AS bonus_granted,
    postingcompleted as granted,
    amount,
    eurexchangerate,
    currency,
    type
  FROM psl.posting
  LEFT JOIN type
  ON CAST(TRIM(RIGHT(note, 13)) AS INT64) = type.bonuswalletid
  WHERE note like 'GrantBonus%'   
    AND note is not null
    AND postingtype = 'Bonus'
    AND payitemname = 'UBS'
)

SELECT 
  bonus_granted.userid, 
  bonus_granted.bonuswalletid, 
  CASE
    WHEN bonus_status = 'released' AND type.type IS NULL
    THEN 'WR0'
    ELSE bonus_status
  END as bonus_status,
  bonus_granted.bonus_granted,
  bonus_granted.granted,
  CASE
    WHEN bonus_status = 'released' or bonus_status = 'used-up'
    THEN bonus_granted.amount
    ELSE bonus_granted.amount - master.amount
  END as amount,
  master.eurexchangerate,
  CASE
    WHEN bonus_status = 'released' or bonus_status = 'used-up'
    THEN bonus_granted.bonus_granted
    ELSE bonus_granted.bonus_granted - (master.amount * master.eurexchangerate)
  END as amounteur,
  master.postingcompleted,
  CASE
    WHEN type.type IS NULL
    THEN 'na'
    ELSE type.type
  END AS type
FROM bonus_granted
LEFT JOIN master
ON bonus_granted.bonuswalletid = master.bonuswalletid

{% if is_incremental() %}
        -- recalculate yesterday + today
        where DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
