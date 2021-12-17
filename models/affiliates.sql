{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "date",
      "data_type": "date"
    },
    partitions = partitions_to_replace
)}}


WITH master AS (
WITH master AS (

WITH joins AS (

SELECT userid, DATE(postingcompleted) as date 
FROM psl.posting
GROUP BY 1, 2
), 

ggr_casino AS (

SELECT userid, date, SUM(turnover) as bets, SUM(ggr) as ggr
FROM {{ref ('fact_ggr') }}
WHERE gamename <> 'Sports Betting'
GROUP BY 1, 2
),

ggr_sport AS (

SELECT userid, date, SUM(turnover) as bets, SUM(ggr) as ggr
FROM {{ref ('fact_ggr') }}
WHERE gamename = 'Sports Betting'
GROUP BY 1, 2
),

bonus_granted as ( 

SELECT userid, SUM(bonus_granted) as bg_eur, CAST(granted AS DATE) AS granted
FROM {{ref ('bonus_costs') }}
GROUP BY userid,granted
),

bonus_costs AS (

SELECT userid,
  DATE(postingcompleted) as date,
  SUM (
    CASE 
      WHEN type = 'casino'
      THEN amounteur
      ELSE 0
    END)
  as bc_casino,
  SUM (
    CASE 
      WHEN type = 'sport'
      THEN amounteur
      ELSE 0
    END)
  as bc_sport
FROM {{ref ('bonus_costs') }}
GROUP BY 1, 2
), 

trans AS (

SELECT userid,
  SUM (CASE
    WHEN transactiontype = 'Deposit'
    THEN amounteur
    ELSE 0
  END) as deposit,
  SUM (CASE
    WHEN transactiontype = 'Withdraw'
    THEN amounteur
    ELSE 0
  END) as withdraw,
  CAST (transactioncompleted AS DATE) AS transactioncompleted
FROM {{ref ('transactions') }}
GROUP BY userid, transactioncompleted),

misc AS ( 

WITH users AS (
  SELECT *,
    ROW_NUMBER () OVER (PARTITION BY userid ORDER BY updated DESC) as rn
  FROM psl.users)
SELECT *
FROM users
WHERE rn = 1
)


SELECT 
  joins.userid,
  misc.username,
  misc.country,
  misc.registrationdate, 
  CASE 
    WHEN SPLIT(misc.affiliatemarker, "_")[OFFSET(0)]=REGEXP_EXTRACT(misc.affiliatemarker,r'([0-9]+)') 
    THEN SPLIT(misc.affiliatemarker, "_")[OFFSET(0)]
    WHEN SPLIT(misc.affiliatemarker, "-")[OFFSET(0)]=REGEXP_EXTRACT(misc.affiliatemarker,r'([0-9]+)') 
    THEN SPLIT(misc.affiliatemarker, "-")[OFFSET(0)] 
    ELSE "Organic" 
  END AS affiliatecode,
  joins.date,
  IFNULL(deposit, 0) as deposit,
  IFNULL(withdraw, 0) as withdraw, 
  IFNULL(bonus_granted.bg_eur, 0) as bonus_granted,
  IFNULL(ggr_casino.bets, 0) + IFNULL(ggr_sport.bets, 0) as bets,
  IFNULL(ggr_casino.ggr, 0) + IFNULL(ggr_sport.ggr, 0) as ggr,
  IFNULL(ggr_casino.ggr, 0) + IFNULL(ggr_sport.ggr, 0) - IFNULL(bonus_costs.bc_casino, 0) - IFNULL(bonus_costs.bc_sport, 0) as ngr,
  IFNULL(ggr_casino.bets, 0) as bets_casino,
  IFNULL(ggr_casino.ggr, 0) as ggr_casino,
  IFNULL(ggr_casino.ggr, 0) - IFNULL(bonus_costs.bc_casino, 0) as ngr_casino,
  IFNULL(ggr_sport.bets, 0) as bets_sport,
  IFNULL(ggr_sport.ggr, 0) as ggr_sport,
  IFNULL(ggr_sport.ggr, 0) - IFNULL(bonus_costs.bc_sport, 0) as ngr_sport
  

FROM joins
LEFT JOIN trans
ON joins.userid = trans.userid AND joins.date = trans.transactioncompleted
LEFT JOIN ggr_casino
ON joins.userid = ggr_casino.userid AND joins.date = ggr_casino.date
LEFT JOIN ggr_sport
ON joins.userid = ggr_sport.userid AND joins.date = ggr_sport.date
LEFT JOIN bonus_granted
ON joins.userid = bonus_granted.userid AND joins.date = bonus_granted.granted
LEFT JOIN bonus_costs
ON joins.userid = bonus_costs.userid AND joins.date = bonus_costs.date
JOIN misc
ON joins.userid = misc.userid
)
SELECT *
FROM master
WHERE deposit != 0 or bets != 0 or ggr !=0 or (ngr_sport + ngr_casino) != 0 or withdraw != 0 or bonus_granted != 0)

SELECT *
FROM master

{% if is_incremental() %}
        -- recalculate yesterday + today
        where date >= CURRENT_DATE() -1
    {% endif %}
