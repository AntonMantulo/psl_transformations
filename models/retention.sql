SELECT '0' as month,

(SELECT COUNT(DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -31 AND CURRENT_DATE() -1
  AND userid IN (
    SELECT userid
    FROM {{ref ('transactions') }}
    WHERE transactiontype = 'Deposit'
      AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -61 AND CURRENT_DATE() -31))
      
/

(SELECT COUNT (DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -61 AND CURRENT_DATE() -31) * 100 as retained_pct,
  
UNION ALL

SELECT '1' as month,

(SELECT COUNT(DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -61 AND CURRENT_DATE() -31
  AND userid IN (
    SELECT userid
    FROM {{ref ('transactions') }}
    WHERE transactiontype = 'Deposit'
      AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -91 AND CURRENT_DATE() -61))
      
/

(SELECT COUNT (DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -91 AND CURRENT_DATE() -61) * 100 as retained_pct,
  
  
UNION ALL 


SELECT '2' as month,

(SELECT COUNT(DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -91 AND CURRENT_DATE() -61
  AND userid IN (
    SELECT userid
    FROM {{ref ('transactions') }}
    WHERE transactiontype = 'Deposit'
      AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -121 AND CURRENT_DATE() -91))
      
/

(SELECT COUNT (DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -91 AND CURRENT_DATE() -61) * 100 as retained_pct,
  
  
UNION ALL

SELECT '3' as month,

(SELECT COUNT(DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -121 AND CURRENT_DATE() -91
  AND userid IN (
    SELECT userid
    FROM {{ref ('transactions') }}
    WHERE transactiontype = 'Deposit'
      AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -151 AND CURRENT_DATE() -121))
      
/

(SELECT COUNT (DISTINCT userid)
FROM {{ref ('transactions') }}
WHERE transactiontype = 'Deposit'
  AND DATE(transactioncompleted) BETWEEN CURRENT_DATE() -121 AND CURRENT_DATE() -91) * 100 as retained_pct
  
ORDER BY 1
