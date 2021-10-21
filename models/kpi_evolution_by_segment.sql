SELECT 'New players' as Segment,
  '0' as sm , 
  ROUND(SUM(bets),2) as turnover, 
  ROUND(SUM(ngr),2) as ngr, 
  ROUND(SUM(deposit),2) as deposits
FROM {{ref ('affiliates') }}
WHERE DATE(registrationdate) BETWEEN CURRENT_DATE()-31 AND CURRENT_DATE()-1
  AND DATE(date) BETWEEN CURRENT_DATE()-31 AND CURRENT_DATE()-1
  
UNION ALL

SELECT 'New players' as Segmeant,
  '1' as sm , 
  ROUND(SUM(bets),2) as turnover, 
  ROUND(SUM(ngr),2) as ngr, 
  ROUND(SUM(deposit),2) as deposits
FROM {{ref ('affiliates') }}
WHERE DATE(registrationdate) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-31
  AND DATE(date) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-31
  
UNION ALL

SELECT 'New players' as Segmeant,
  '2' as sm , 
  ROUND(SUM(bets),2) as turnover, 
  ROUND(SUM(ngr),2) as ngr, 
  ROUND(SUM(deposit),2) as deposits
FROM {{ref ('affiliates') }}
WHERE DATE(registrationdate) BETWEEN CURRENT_DATE()-91 AND CURRENT_DATE()-61
  AND DATE(date) BETWEEN CURRENT_DATE()-91 AND CURRENT_DATE()-61
  
UNION ALL

SELECT 'Reactivated players' AS Segment,
  '0' as sm,
  ROUND(SUM(turnover),2) as turnover, 
  ROUND(SUM(ngr),2) as ngr, 
  ROUND(SUM(deposits),2) as deposits
FROM (
  SELECT userid, 
    SUM(bets) as turnover, 
    SUM(ngr) as ngr, 
    SUM(deposit) as deposits
  FROM {{ref ('affiliates') }}
  WHERE DATE(date) BETWEEN CURRENT_DATE()-31 AND CURRENT_DATE()-1 
    AND userid IN (
      SELECT userid 
      FROM {{ref ('affiliates') }}
      WHERE DATE(date) BETWEEN CURRENT_DATE()-211 
        AND CURRENT_DATE()-61 and (deposit != 0 or deposit IS NOT NULL)
      )

   AND userid not in (
      SELECT userid 
      FROM {{ref ('affiliates') }}
      WHERE DATE(date) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-31 AND deposit > 0 
      )

  GROUP BY 1
  HAVING SUM(deposit) > 0) as react_1
GROUP BY 1, 2

UNION ALL

SELECT 'Reactivated players' AS Segment,
  '1' as sm,
  ROUND(SUM(turnover),2) as turnover, 
  ROUND(SUM(ngr),2) as ngr, 
  ROUND(SUM(deposits),2) as deposits
FROM (
  SELECT userid, 
    SUM(bets) as turnover, 
    SUM(ngr) as ngr, 
    SUM(deposit) as deposits
  FROM {{ref ('affiliates') }}
  WHERE DATE(date) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-31 
    AND userid IN (
      SELECT userid 
      FROM {{ref ('affiliates') }}
      WHERE DATE(date) BETWEEN CURRENT_DATE()-241 
        AND CURRENT_DATE()-91 and (deposit != 0 or deposit IS NOT NULL)
      )

   AND userid not in (
      SELECT userid 
      FROM {{ref ('affiliates') }}
      WHERE DATE(date) BETWEEN CURRENT_DATE()-91 AND CURRENT_DATE()-61 AND deposit > 0 
      )

  GROUP BY 1
  HAVING SUM(deposit) > 0) as react_1
GROUP BY 1, 2

UNION ALL

SELECT 'Reactivated players' AS Segment,
  '2' as sm,
  ROUND(SUM(turnover), 2) as turnover, 
  ROUND(SUM(ngr), 2) as ngr, 
  ROUND(SUM(deposits), 2) as deposits
FROM (
  SELECT userid, 
    SUM(bets) as turnover, 
    SUM(ngr) as ngr, 
    SUM(deposit) as deposits
  FROM {{ref ('affiliates') }}
  WHERE DATE(date) BETWEEN CURRENT_DATE()-91 AND CURRENT_DATE()-61 
    AND userid IN (
      SELECT userid 
      FROM {{ref ('affiliates') }}
      WHERE DATE(date) BETWEEN CURRENT_DATE()-271 
        AND CURRENT_DATE()-121 and (deposit != 0 or deposit IS NOT NULL)
      )

   AND userid not in (
      SELECT userid 
      FROM {{ref ('affiliates') }}
      WHERE DATE(date) BETWEEN CURRENT_DATE()-121 AND CURRENT_DATE()-91 AND deposit > 0 
      )

  GROUP BY 1
  HAVING SUM(deposit) > 0) as react_1
GROUP BY 1, 2

UNION ALL 

SELECT 'Retained players' AS Segment,
  '0' as sm,
  ROUND(SUM(turnover), 2) as turnover, 
  ROUND(SUM(ngr), 2) as ngr, 
  ROUND(SUM(deposits), 2) as deposits
FROM (
  SELECT userid, 
    SUM(bets) as turnover, 
    SUM(ngr) as ngr, 
  SUM(deposit) as deposits
  FROM {{ref ('affiliates') }}
  WHERE DATE(date) BETWEEN CURRENT_DATE()-31 AND CURRENT_DATE()-1 
    AND userid IN (
      SELECT userid 
      FROM {{ref ('affiliates') }} 
      WHERE DATE(date) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-31 
        AND (deposit != 0 or deposit IS NOT NULL)
     )

  GROUP BY 1
  HAVING SUM(deposit) > 0) AS retained
GROUP BY 1, 2

UNION ALL 

SELECT 'Retained players' AS Segment,
  '1' as sm,
  ROUND(SUM(turnover), 2) as turnover, 
  ROUND(SUM(ngr), 2) as ngr, 
  ROUND(SUM(deposits), 2) as deposits
FROM (
  SELECT userid, 
    SUM(bets) as turnover, 
    SUM(ngr) as ngr, 
  SUM(deposit) as deposits
  FROM {{ref ('affiliates') }}
  WHERE DATE(date) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-31 
    AND userid IN (
      SELECT userid 
      FROM {{ref ('affiliates') }} 
      WHERE DATE(date) BETWEEN CURRENT_DATE()-91 AND CURRENT_DATE()-61 
        AND (deposit != 0 or deposit IS NOT NULL)
     )

  GROUP BY 1
  HAVING SUM(deposit) > 0) AS retained
GROUP BY 1, 2

UNION ALL 

SELECT 'Retained players' AS Segment,
  '2' as sm,
  ROUND(SUM(turnover), 2) as turnover, 
  ROUND(SUM(ngr), 2) as ngr, 
  ROUND(SUM(deposits), 2) as deposits
FROM (
  SELECT userid, 
    SUM(bets) as turnover, 
    SUM(ngr) as ngr, 
  SUM(deposit) as deposits
  FROM dbt_psl.affiliates
  WHERE DATE(date) BETWEEN CURRENT_DATE()-91 AND CURRENT_DATE()-61 
    AND userid IN (
      SELECT userid 
      FROM dbt_psl.affiliates 
      WHERE DATE(date) BETWEEN CURRENT_DATE()-121 AND CURRENT_DATE()-91 
        AND (deposit != 0 or deposit IS NOT NULL)
     )

  GROUP BY 1
  HAVING SUM(deposit) > 0) AS retained
GROUP BY 1, 2
