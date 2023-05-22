WITH SQLStatement_1 AS (

  SELECT 
    substr(w_warehouse_name, 1, 20),
    sm_type,
    cc_name,
    sum(CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30)
        THEN 1
      ELSE 0
    END) AS t_30days,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) and
         (cs_ship_date_sk - cs_sold_date_sk <= 60)
          THEN 1
        ELSE 0
      END) AS t_31_60_days,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) and
         (cs_ship_date_sk - cs_sold_date_sk <= 90)
          THEN 1
        ELSE 0
      END) AS t_61_90_days,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) and
         (cs_ship_date_sk - cs_sold_date_sk <= 120)
          THEN 1
        ELSE 0
      END) AS t_91_120_days,
    sum(CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 120)
        THEN 1
      ELSE 0
    END) AS t_morethan_120days
  
  FROM spark_catalog.qa_performance_database.catalog_sales, spark_catalog.qa_performance_database.warehouse, spark_catalog.qa_performance_database.ship_mode, spark_catalog.qa_performance_database.call_center, spark_catalog.qa_performance_database.date_dim
  
  WHERE d_month_seq BETWEEN 1200 and 1200 + 11 and cs_ship_date_sk = d_date_sk and cs_warehouse_sk = w_warehouse_sk and cs_ship_mode_sk = sm_ship_mode_sk and cs_call_center_sk = cc_call_center_sk
  
  GROUP BY 
    substr(w_warehouse_name, 1, 20), sm_type, cc_name
  
  ORDER BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
  
  LIMIT 100

),

tpcds_perf_sql_0 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_0')}}

),

Join_1 AS (

  SELECT 
    in0.sm_type AS sm_type,
    in0.cc_name AS cc_name,
    in0.t_30days AS t_30days,
    in0.t_31_60_days AS t_31_60_days,
    in0.t_61_90_days AS t_61_90_days,
    in0.t_91_120_days AS t_91_120_days,
    in0.t_morethan_120days AS t_morethan_120days,
    in1.Excess_Discount_Amount AS Excess_Discount_Amount,
    in1.Excess_Discount_Description AS Excess_Discount_Description
  
  FROM SQLStatement_1 AS in0
  INNER JOIN tpcds_perf_sql_0 AS in1
     ON in0.cc_name != in1.Excess_Discount_Description

),

tpcds_perf_sql_1 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_1')}}

),

tpcds_perf_sql_2_1 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_2')}}

),

Join_2 AS (

  SELECT 
    tpcds_perf_sql_2_1.a_30_days AS a_30_days,
    tpcds_perf_sql_2_1.a_31_60_days AS a_31_60_days,
    tpcds_perf_sql_2_1.a_61_90_days AS a_61_90_days,
    tpcds_perf_sql_2_1.a_91_120_days AS a_91_120_days,
    tpcds_perf_sql_2_1.a_120_days_and_more AS a_120_days_and_more,
    tpcds_perf_sql_1.h10_30_to_11 AS h10_30_to_11,
    tpcds_perf_sql_1.h9_30_to_10 AS h9_30_to_10,
    tpcds_perf_sql_1.h9_to_9_30 AS h9_to_9_30,
    tpcds_perf_sql_1.days_31_60 AS days_31_60,
    tpcds_perf_sql_1.days_30 AS days_30,
    tpcds_perf_sql_1.i_current_price AS i_current_price,
    tpcds_perf_sql_1.i_item_id AS i_item_id,
    tpcds_perf_sql_1.days_61_90 AS days_61_90,
    tpcds_perf_sql_1.i_item_desc AS i_item_desc,
    tpcds_perf_sql_1.revenueratio AS revenueratio,
    tpcds_perf_sql_1.days_more_than_120 AS days_more_than_120,
    tpcds_perf_sql_1.h11_to_11_30 AS h11_to_11_30,
    tpcds_perf_sql_1.sm_type AS sm_type,
    tpcds_perf_sql_1.i_category AS i_category,
    tpcds_perf_sql_1.h11_30_to_12 AS h11_30_to_12,
    tpcds_perf_sql_1.i_class AS i_class,
    tpcds_perf_sql_1.cc_name AS cc_name,
    tpcds_perf_sql_1.itemrevenue AS itemrevenue,
    tpcds_perf_sql_1.h12_to_12_30 AS h12_to_12_30,
    tpcds_perf_sql_1.h8_30_to_9 AS h8_30_to_9,
    tpcds_perf_sql_1.h10_to_10_30 AS h10_to_10_30,
    tpcds_perf_sql_1.days_90_120 AS days_90_120
  
  FROM tpcds_perf_sql_1
  INNER JOIN tpcds_perf_sql_2_1
     ON tpcds_perf_sql_1.sm_type = tpcds_perf_sql_2_1.sm_type
  INNER JOIN Join_1 AS in2
     ON tpcds_perf_sql_2_1.sm_type != in2.sm_type

)

SELECT *

FROM Join_2
