WITH test_perf_model_0 AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_0')}}

),

test_perf_model_2 AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_2')}}

),

test_perf_model_3_use_all AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_3_use_all')}}

),

test_perf_model_1 AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_1')}}

),

tpcds_perf_sql_0 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_0')}}

),

Join_1 AS (

  SELECT 
    in0.city AS city,
    in0.state AS state,
    in0.pin AS pin,
    in0.p_int AS p_int,
    in0.p_string AS p_string,
    in0.c_string AS c_string,
    in0.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_smallint AS c_smallint,
    in0.c_tinyint AS c_tinyint,
    in0.c_float AS c_float,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_double AS c_double,
    in0.c_struct AS c_struct
  
  FROM test_perf_model_3_use_all AS in0
  RIGHT JOIN test_perf_model_0 AS in1
     ON in0.p_string != in1.Industry_aggregation_NZSIOC
  LEFT JOIN test_perf_model_1 AS in2
     ON in1.Variable_code_1 != in2.p_string
  SEMI JOIN test_perf_model_2 AS in3
     ON in2.p_string != in3.Series_reference

),

Limit_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  LIMIT 10

),

tpcds_perf_sql_2 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_2')}}

),

tpcds_perf_sql_1 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_1')}}

),

tpcds_perf_sql_3_useall_tpcds AS (

  SELECT * 
  
  FROM {{ ref('tpcds_perf_sql_3_useall_tpcds')}}

),

Join_2 AS (

  SELECT 
    in2.sm_type AS sm_type,
    in2.cc_name AS cc_name,
    in2.days_30 AS days_30,
    in2.days_31_60 AS days_31_60,
    in2.days_61_90 AS days_61_90,
    in2.days_90_120 AS days_90_120,
    in2.days_more_than_120 AS days_more_than_120,
    in2.i_item_id AS i_item_id,
    in2.i_item_desc AS i_item_desc,
    in2.i_category AS i_category,
    in2.i_class AS i_class,
    in2.i_current_price AS i_current_price,
    in2.itemrevenue AS itemrevenue,
    in2.revenueratio AS revenueratio,
    in2.h8_30_to_9 AS h8_30_to_9,
    in2.h9_to_9_30 AS h9_to_9_30,
    in2.h9_30_to_10 AS h9_30_to_10,
    in2.h10_to_10_30 AS h10_to_10_30,
    in2.h10_30_to_11 AS h10_30_to_11,
    in2.h11_to_11_30 AS h11_to_11_30,
    in2.h11_30_to_12 AS h11_30_to_12,
    in2.h12_to_12_30 AS h12_to_12_30
  
  FROM tpcds_perf_sql_2 AS in0
  INNER JOIN tpcds_perf_sql_3_useall_tpcds AS in1
     ON in0.sm_type != in1.i_item_desc
  LEFT JOIN tpcds_perf_sql_1 AS in2
     ON in1.i_item_desc != in2.cc_name
  RIGHT JOIN tpcds_perf_sql_0 AS in3
     ON in2.cc_name != in3.Excess_Discount_Description

),

Limit_1_1 AS (

  SELECT * 
  
  FROM Join_2 AS in0
  
  LIMIT 10

),

Join_3 AS (

  SELECT 
    in0.city AS city,
    in0.state AS state,
    in0.pin AS pin,
    in0.p_int AS p_int,
    in0.p_string AS p_string,
    in0.c_string AS c_string,
    in0.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_smallint AS c_smallint,
    in0.c_tinyint AS c_tinyint,
    in0.c_float AS c_float,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_double AS c_double,
    in0.c_struct AS c_struct
  
  FROM Limit_1 AS in0
  INNER JOIN Limit_1_1 AS in1
     ON in0.city != in1.itemrevenue

)

SELECT *

FROM Join_3
