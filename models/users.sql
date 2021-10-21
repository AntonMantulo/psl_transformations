
WITH master AS (
WITH users AS (
  SELECT userid,
    username,
    affiliatemarker,
    country,
    registrationdate,
    ROW_NUMBER () OVER (PARTITION BY userid ORDER BY updated DESC) as rn

  FROM `stitch-test-296708.psl.users`
)

SELECT 
  users.userid,
  username,
  affiliatemarker,
  country,
  registrationdate,
  SUM (
    CASE 
      WHEN gamename = 'Sports Betting'
      THEN 0
      ELSE ggr
    END) AS ggr_casino,
  SUM (
    CASE 
      WHEN gamename = 'Sports Betting'
      THEN ggr
      ELSE 0
    END) AS ggr_sport,
  SUM (turnover) AS turnover,
  SUM (ggr) AS ggr

FROM users
LEFT JOIN {{ref ('fact_ggr') }}
ON users.userid = fact_ggr.userid
WHERE users.rn = 1
GROUP BY 1, 2, 3, 4, 5
),

deposits AS (
SELECT userid, SUM(amounteur) AS deposits
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
GROUP BY 1
),

t1 AS (
WITH deps as (
SELECT userid, transactioncompleted, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY transactioncompleted ASC) as rn
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit')
SELECT *
FROM deps 
WHERE rn = 1
),

t2 as (
WITH deps as (
SELECT userid, transactioncompleted, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY transactioncompleted DESC) as rn
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit')
SELECT *
FROM deps 
WHERE rn = 1
),

bonus_costs AS (
SELECT userid, 
  SUM (
    CASE
      WHEN type = 'casino'
      THEN bonus_costs.amounteur
      ELSE 0 
    END) AS bc_casino,
  SUM (
    CASE
      WHEN type = 'sport'
      THEN bonus_costs.amounteur
      ELSE 0 
    END) AS bc_sport,
  SUM (amounteur) as bc
FROM {{ref ('bonus_costs') }}
GROUP BY 1
)
  

SELECT 
  master.userid,
  username,
  CASE 
    WHEN SPLIT(affiliatemarker, "_")[OFFSET(0)]=REGEXP_EXTRACT(affiliatemarker,r'([0-9]+)') 
    THEN SPLIT(affiliatemarker, "_")[OFFSET(0)]
    WHEN SPLIT(affiliatemarker, "-")[OFFSET(0)]=REGEXP_EXTRACT(affiliatemarker,r'([0-9]+)') 
    THEN SPLIT(affiliatemarker, "-")[OFFSET(0)] 
    ELSE "Organic" 
  END AS affiliatemarker,
  country,
  registrationdate,
  deposits.deposits,
  t1.transactioncompleted AS first_deposit,
  t2.transactioncompleted AS last_deposit,
  turnover,
  bonus_costs.bc as bonus_costs,
  ggr,
  ggr - bc as ngr,
  ggr_casino,
  ggr_sport,
  ggr_casino - bc_casino as ngr_casino,
  ggr_sport - bc_sport as ngr_sport
FROM master
LEFT JOIN deposits
ON master.userid = deposits.userid
LEFT JOIN t1 
ON master.userid = t1.userid
LEFT JOIN t2 
ON master.userid = t2.userid
LEFT JOIN bonus_costs
ON master.userid = bonus_costs.userid
