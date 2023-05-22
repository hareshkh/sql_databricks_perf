WITH SQLStatement_1 AS (

  SELECT sum(ws_ext_discount_amt) AS Excess_Discount_Amount
  
  FROM spark_catalog.qa_performance_database.web_sales, spark_catalog.qa_performance_database.item, spark_catalog.qa_performance_database.date_dim
  
  WHERE i_manufact_id = 939 and i_item_sk = ws_item_sk and d_date BETWEEN '2002-02-16' and
   dateadd(DAY, 90, to_date('2002-02-16')) and d_date_sk = ws_sold_date_sk and ws_ext_discount_amt > (
    SELECT 1.3 * avg(ws_ext_discount_amt)
    
    FROM spark_catalog.qa_performance_database.web_sales, spark_catalog.qa_performance_database.date_dim
    
    WHERE ws_item_sk = i_item_sk and d_date BETWEEN '2002-02-16' and
     dateadd(DAY, 90, to_date('2002-02-16')) and d_date_sk = ws_sold_date_sk
   )
  
  ORDER BY sum(ws_ext_discount_amt)
  
  LIMIT 100

),

Reformat_1 AS (

  SELECT 
    Excess_Discount_Amount AS Excess_Discount_Amount,
    concat('Discount is:', Excess_Discount_Amount) AS Excess_Discount_Description
  
  FROM SQLStatement_1 AS in0

)

SELECT *

FROM Reformat_1
