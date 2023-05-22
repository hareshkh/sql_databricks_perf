WITH warehouse AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'warehouse') }}

),

call_center AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'call_center') }}

),

ship_mode AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'ship_mode') }}

),

item AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'item') }}

),

date_dim AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'date_dim') }}

),

SQLStatement_2 AS (

  SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    sum(ss_ext_sales_price) AS itemrevenue,
    sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
  
  FROM spark_catalog.qa_performance_database.store_sales, item, date_dim
  
  WHERE ss_item_sk = i_item_sk and i_category IN ('Women', 'Electronics', 'Shoes') and ss_sold_date_sk = d_date_sk and d_date BETWEEN CAST('2002-05-27' AS date) and DATE_ADD(DATE'2002-05-27', 10)
  
  GROUP BY 
    i_item_id, i_item_desc, i_category, i_class, i_current_price
  
  ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio

),

catalog_sales AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'catalog_sales') }}

),

SQLStatement_1 AS (

  SELECT 
    substr(w_warehouse_name, 1, 20),
    sm_type,
    cc_name,
    sum(CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30)
        THEN 1
      ELSE 0
    END) AS a_30_days,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) and
         (cs_ship_date_sk - cs_sold_date_sk <= 60)
          THEN 1
        ELSE 0
      END) AS a_31_60_days,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) and
         (cs_ship_date_sk - cs_sold_date_sk <= 90)
          THEN 1
        ELSE 0
      END) AS a_61_90_days,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) and
         (cs_ship_date_sk - cs_sold_date_sk <= 120)
          THEN 1
        ELSE 0
      END) AS a_91_120_days,
    sum(CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 120)
        THEN 1
      ELSE 0
    END) AS a_120_days_and_more
  
  FROM catalog_sales, warehouse, ship_mode, call_center, date_dim
  
  WHERE d_month_seq BETWEEN 1200 and 1200 + 11 and cs_ship_date_sk = d_date_sk and cs_warehouse_sk = w_warehouse_sk and cs_ship_mode_sk = sm_ship_mode_sk and cs_call_center_sk = cc_call_center_sk
  
  GROUP BY 
    substr(w_warehouse_name, 1, 20), sm_type, cc_name
  
  ORDER BY sm_type, cc_name
  
  LIMIT 100

),

Join_1 AS (

  SELECT 
    in0.sm_type AS sm_type,
    in0.cc_name AS cc_name,
    in0.a_30_days AS a_30_days,
    in0.a_31_60_days AS a_31_60_days,
    in0.a_61_90_days AS a_61_90_days,
    in0.a_91_120_days AS a_91_120_days,
    in0.a_120_days_and_more AS a_120_days_and_more,
    in1.i_item_id AS i_item_id,
    in1.i_item_desc AS i_item_desc,
    in1.i_category AS i_category,
    in1.i_class AS i_class,
    in1.i_current_price AS i_current_price,
    in1.itemrevenue AS itemrevenue,
    in1.revenueratio AS revenueratio
  
  FROM SQLStatement_1 AS in0
  INNER JOIN SQLStatement_2 AS in1
     ON in0.SM_TYPE != in1.I_ITEM_DESC

)

SELECT *

FROM Join_1
