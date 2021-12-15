{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
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
WITH bets AS (

SELECT
  userid,
  DATE(postingcompleted) as date,
  gamegroup,
  productname,
  gamename,
  COUNT(*) AS rounds,
  SUM(amounteur) AS turnover
FROM {{ref ('bets') }}
GROUP BY 1, 2, 3, 4, 5
)

SELECT 
  bets.userid,
  date,
  bets.gamegroup,
  bets.productname,
  bets.gamename,
  rounds,
  turnover,
  turnover - IFNULL(SUM(win.amounteur), 0) as ggr
FROM bets
JOIN {{ref ('win') }}
ON bets.userid = win.userid 
  AND date = DATE(postingcompleted)
  AND bets.gamegroup = win.gamegroup
  AND bets.productname = win.productname
  AND bets.gamename = win.gamename 
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1
)
SELECT *
FROM master

{% if is_incremental() %}
        -- recalculate yesterday + today
        where DATE(date) >= CURRENT_DATE() -1
    {% endif %}
