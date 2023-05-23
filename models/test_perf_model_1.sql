WITH all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'all_type_non_partitioned') }}

),

Filter_1 AS (

  SELECT * 
  
  FROM all_type_non_partitioned AS in0
  
  WHERE true

),

Limit_1 AS (

  SELECT * 
  
  FROM Filter_1 AS in0
  
  LIMIT 10

),

all_type_partitioned AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'all_type_partitioned') }}

),

Aggregate_1 AS (

  SELECT 
    any_value(c_tinyint) AS c_tinyint,
    any_value(c_smallint) AS c_smallint,
    any_value(c_int) AS c_int,
    any_value(c_bigint) AS c_bigint,
    any_value(c_float) AS c_float,
    any_value(c_double) AS c_double,
    any_value(c_string) AS c_string,
    any_value(c_boolean) AS c_boolean,
    any_value(c_struct.city) AS c_struct_city,
    any_value(c_struct.state) AS c_struct_state,
    any_value(p_int) AS p_int,
    any_value(p_string) AS p_string
  
  FROM all_type_partitioned AS in0
  
  GROUP BY c_string

),

OrderBy_1 AS (

  SELECT * 
  
  FROM Limit_1 AS in0
  
  ORDER BY c_tinyint ASC NULLS FIRST, c_smallint ASC NULLS LAST, c_int DESC NULLS FIRST, c_bigint DESC NULLS LAST, c_float ASC, c_double ASC

),

Reformat_1 AS (

  SELECT 
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    c_struct.city AS city,
    c_struct.state AS state,
    c_struct.pin AS pin,
    {{ Perf_SQL_Databricks._concat_2_macro('c_string') }} AS c_macro
  
  FROM OrderBy_1 AS in0

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in0.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct,
    in0.city AS city,
    in0.state AS state,
    in0.pin AS pin,
    in1.p_int AS p_int,
    in1.p_string AS p_string
  
  FROM Reformat_1 AS in0
  INNER JOIN Aggregate_1 AS in1
     ON in0.c_tinyint = in1.c_tinyint

)

SELECT *

FROM Join_1
