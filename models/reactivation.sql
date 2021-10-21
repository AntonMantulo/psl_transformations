WITH master as (
WITH master AS (
WITH react as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-36 AND CURRENT_DATE()-6
    AND transactiontype="Deposit"
    GROUP BY 1),
     
  all_data as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react.react IS NULL then 1 else 0 END AS react1
    FROM {{ref ('users') }} as users 
    LEFT JOIN react
    ON users.userid=react.userid),
        
        
  react_1 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react_1
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-43 AND CURRENT_DATE()-13
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  deposit AS (
    SELECT userid,
    COUNT(transactioncompleted) AS dep
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-7
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  trans as (
    SELECT userid, 
    COUNT(transactioncompleted) AS trans
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-193 AND CURRENT_DATE()-43
    AND transactiontype="Deposit"
    GROUP BY 1  )  ,  
    
  trans2 AS (
    SELECT users.userid,
    CASE WHEN  react_1.react_1 IS NULL then 1 else 0 END AS react1_1,
    CASE WHEN trans.trans IS NULL then 1 else 0 END AS trans
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_1
    ON users.userid=react_1.userid
    LEFT JOIN trans 
    ON users.userid=trans.userid  ) ,
     
  all_data_1 as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react_1.react_1 IS NULL then 1 else 0 END AS react1_1,
    CASE WHEN deposit.dep IS NULL then 1 else 0 END AS dep
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_1
    ON users.userid=react_1.userid
    LEFT JOIN deposit 
    ON users.userid=deposit.userid),
                
  react_2 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react_2
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-50 AND CURRENT_DATE()-20
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  deposit2 AS (
    SELECT userid,
    COUNT(transactioncompleted) AS dep2
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-20 AND CURRENT_DATE()-14
    AND transactiontype="Deposit"
    GROUP BY 1),
            
  trans1 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS trans1
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-200 AND CURRENT_DATE()-50
    AND transactiontype="Deposit"
    GROUP BY 1),
                        
  trans3 AS (
    SELECT users.userid,
    CASE WHEN  react_2.react_2 IS NULL then 1 else 0 END AS react_2,
    CASE WHEN trans1.trans1 IS NULL then 1 else 0 END AS trans1
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_2
    ON users.userid=react_2.userid
    LEFT JOIN trans1 
    ON users.userid=trans1.userid  ),
               
  all_data_2 as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react_2.react_2 IS NULL then 1 else 0 END AS react_2,
    CASE WHEN deposit2.dep2 IS NULL then 1 else 0 END AS dep2
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_2
    ON users.userid=react_2.userid
    LEFT JOIN deposit2 
    ON users.userid=deposit2.userid),
                
  react_3 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react_3
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-57 AND CURRENT_DATE()-27
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  deposit3 AS (
    SELECT userid,
    COUNT(transactioncompleted) AS dep3
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-27 AND CURRENT_DATE()-21
    AND transactiontype="Deposit"
    GROUP BY 1),
                 
  trans_2 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS trans_2
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-207 AND CURRENT_DATE()-57
    AND transactiontype="Deposit"
    GROUP BY 1  )  ,   
        
  trans4 AS (
    SELECT users.userid,
    CASE WHEN  react_3.react_3 IS NULL then 1 else 0 END AS react_3,
    CASE WHEN trans_2.trans_2 IS NULL then 1 else 0 END AS trans_2
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_3
    ON users.userid=react_3.userid
    LEFT JOIN trans_2 
    ON users.userid=trans_2.userid),
                
  all_data_3 as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react_3.react_3 IS NULL then 1 else 0 END AS react_3,
    CASE WHEN deposit3.dep3 IS NULL then 1 else 0 END AS dep3
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_3
    ON users.userid=react_3.userid
    LEFT JOIN deposit3 
    ON users.userid=deposit3.userid), 
                
  react_4 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react_4
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-64 AND CURRENT_DATE()-34
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  deposit4 AS (
    SELECT userid,
    COUNT(transactioncompleted) AS dep4
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-34 AND CURRENT_DATE()-28
    AND transactiontype="Deposit"
    GROUP BY 1),
     
  trans_3 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS trans_3
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-214 AND CURRENT_DATE()-64
    AND transactiontype="Deposit"
    GROUP BY 1  )  ,  
    
  trans5 AS (
    SELECT users.userid,
    CASE WHEN  react_4.react_4 IS NULL then 1 else 0 END AS react_4,
    CASE WHEN trans_3.trans_3 IS NULL then 1 else 0 END AS trans_3
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_4
    ON users.userid=react_4.userid
    LEFT JOIN trans_3 
    ON users.userid=trans_3.userid  ),
                
  all_data_4 as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react_4.react_4 IS NULL then 1 else 0 END AS react_4,
    CASE WHEN deposit4.dep4 IS NULL then 1 else 0 END AS dep4
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_4
    ON users.userid=react_4.userid
    LEFT JOIN deposit4 
    ON users.userid=deposit4.userid),

  react_5 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react_5
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-71 AND CURRENT_DATE()-41
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  deposit5 AS (
    SELECT userid,
    COUNT(transactioncompleted) AS dep5
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-41 AND CURRENT_DATE()-35
    AND transactiontype="Deposit"
    GROUP BY 1),
     
  trans_4 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS trans_4
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-221 AND CURRENT_DATE()-71
    AND transactiontype="Deposit"
    GROUP BY 1  )  ,
        
  trans6 AS (
    SELECT users.userid,
    CASE WHEN  react_5.react_5 IS NULL then 1 else 0 END AS react_5,
    CASE WHEN trans_4.trans_4 IS NULL then 1 else 0 END AS trans_4
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_5
    ON users.userid=react_5.userid
    LEFT JOIN trans_4 
    ON users.userid=trans_4.userid),
                
  all_data_5 as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react_5.react_5 IS NULL then 1 else 0 END AS react_5,
    CASE WHEN deposit5.dep5 IS NULL then 1 else 0 END AS dep5
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_5
    ON users.userid=react_5.userid
    LEFT JOIN deposit5 
    ON users.userid=deposit5.userid),
                
  react_6 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS react_6
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-78 AND CURRENT_DATE()-48
    AND transactiontype="Deposit"
    GROUP BY 1),
        
  deposit6 AS (
    SELECT userid,
    COUNT(transactioncompleted) AS dep6
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-48 AND CURRENT_DATE()-42
    AND transactiontype="Deposit"
    GROUP BY 1),
     
  trans_5 as (
    SELECT userid, 
    COUNT(transactioncompleted) AS trans_5
    FROM {{ref ('transactions') }}
    WHERE DATE(transactioncompleted) BETWEEN CURRENT_DATE()-228 AND CURRENT_DATE()-78
    AND transactiontype="Deposit"
    GROUP BY 1  )  ,    
        
  trans7 AS (
    SELECT users.userid,
    CASE WHEN  react_6.react_6 IS NULL then 1 else 0 END AS react_6,
    CASE WHEN trans_5.trans_5 IS NULL then 1 else 0 END AS trans_5
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_6
    ON users.userid=react_6.userid
    LEFT JOIN trans_5 
    ON users.userid=trans_5.userid  ) ,
                
  all_data_6 as(
    SELECT users.userid,
    first_deposit,
    last_deposit,
    CASE WHEN  react_6.react_6 IS NULL then 1 else 0 END AS react_6,
    CASE WHEN deposit6.dep6 IS NULL then 1 else 0 END AS dep6
    FROM {{ref ('users') }} as users 
    LEFT JOIN react_6
    ON users.userid=react_6.userid
    LEFT JOIN deposit6 
    ON users.userid=deposit6.userid)

            
SELECT COUNT(userid) ,

((SELECT COUNT(userid) from all_data WHERE react1=1  and (DATE(last_deposit)BETWEEN CURRENT_DATE()-6 AND CURRENT_DATE()-1) AND DATE(first_deposit)<=(CURRENT_DATE()-36))
/
(SELECT COUNT(userid) AS total FROM {{ref ('users') }} WHERE DATE(last_deposit) between CURRENT_DATE()-186 and CURRENT_DATE()-36))*100 as reactivated_last_week
,

((SELECT COUNT(userid) FROM all_data_1 WHERE react1_1=1 and dep=0 and DATE(first_deposit)<=CURRENT_DATE()-43)
/
(SELECT COUNT(userid) FROM trans2 WHERE trans=0 AND react1_1=1 ))*100 as reactivated_week_minus_1
,

((SELECT COUNT(userid) FROM all_data_2 WHERE react_2=1 and dep2=0 and DATE(first_deposit)<=CURRENT_DATE()-50)
/
(SELECT COUNT(userid) FROM trans3 WHERE trans1=0 AND react_2=1 ))*100 as reactivated_week_minus_2
,

((SELECT COUNT(userid) FROM all_data_3 WHERE react_3=1 and dep3=0 and DATE(first_deposit)<=CURRENT_DATE()-57)
/
(SELECT COUNT(userid) FROM trans4 WHERE trans_2=0 AND react_3=1 ))*100 as reactivated_week_minus_3
,

((SELECT COUNT(userid) FROM all_data_4 WHERE react_4=1 and dep4=0 and DATE(first_deposit)<=CURRENT_DATE()-64)
/
(SELECT COUNT(userid) FROM trans5 WHERE trans_3=0 AND react_4=1 ))*100 as reactivated_week_minus_4
,

((SELECT COUNT(userid) FROM all_data_5 WHERE react_5=1 and dep5=0 and DATE(first_deposit)<=CURRENT_DATE()-71)
/
(SELECT COUNT(userid) FROM trans6 WHERE trans_4=0 AND react_5=1 ))*100 as reactivated_week_minus_5
,

((SELECT COUNT(userid) FROM all_data_6 WHERE react_6=1 and dep6=0 and DATE(first_deposit)<=CURRENT_DATE()-78)
/
(SELECT COUNT(userid) FROM trans7 WHERE trans_5=0 AND react_6=1 ))*100 as reactivated_week_minus_6
                
FROM {{ref ('users') }})

SELECT *
FROM master 
UNPIVOT (react_pct FOR week IN (reactivated_last_week as '0', reactivated_week_minus_1 as '1', reactivated_week_minus_2 as '2', reactivated_week_minus_3 as '3', reactivated_week_minus_4 as '4', reactivated_week_minus_5 as '5', reactivated_week_minus_6 as '6')))
SELECT week, react_pct
FROM master
