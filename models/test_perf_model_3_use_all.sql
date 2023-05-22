WITH test_perf_model_1 AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_1')}}

),

test_perf_model_2 AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_2')}}

),

Join_2 AS (

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
    in0.p_int AS p_int,
    in0.p_string AS p_string,
    in1.Series_reference AS Series_reference,
    in1.Period_1 AS Period_1,
    in1.Data_value_1 AS Data_value_1,
    in1.Suppressed_1 AS Suppressed_1,
    in1.STATUS_1 AS STATUS_1,
    in1.UNITS_1 AS UNITS_1,
    in1.Magnitude_1 AS Magnitude_1,
    in1.Subject_1 AS Subject_1,
    in1.Group_1 AS Group_1,
    in1.Series_title_1 AS Series_title_1,
    in1.Series_title_2 AS Series_title_2,
    in1.Series_title_3 AS Series_title_3,
    in1.Series_title_4 AS Series_title_4,
    in1.Series_title_5 AS Series_title_5,
    in1.Variable_category_1 AS Variable_category_1,
    in1.Industry_code_NZSIOC AS Industry_code_NZSIOC,
    in1.Year_1 AS Year_1,
    in1.Industry_code_ANZSIC06 AS Industry_code_ANZSIC06,
    in1.Variable_name_1 AS Variable_name_1,
    in1.Industry_aggregation_NZSIOC AS Industry_aggregation_NZSIOC,
    in1.Variable_code_1 AS Variable_code_1,
    in1.Value_1 AS Value_1,
    in1.Industry_name_NZSIOC AS Industry_name_NZSIOC,
    in1.code_1 AS code_1,
    in1.service_label_1 AS service_label_1,
    in1.country_code AS country_code,
    in1.country_label AS country_label
  
  FROM test_perf_model_1 AS in0
  INNER JOIN test_perf_model_2 AS in1
     ON in0.c_string != in1.Subject_1

),

OrderBy_1 AS (

  SELECT * 
  
  FROM Join_2 AS in0
  
  ORDER BY c_tinyint ASC NULLS LAST

),

Filter_1 AS (

  SELECT * 
  
  FROM OrderBy_1 AS in0
  
  WHERE true

),

test_perf_model_0 AS (

  SELECT * 
  
  FROM {{ ref('test_perf_model_0')}}

),

Join_1 AS (

  SELECT 
    annual_enterprise_perf.Units_1 AS Units_1,
    test_perf_model_2.Data_value_1 AS Data_value_1,
    test_perf_model_2.Subject_1 AS Subject_1,
    test_perf_model_2.Series_title_3 AS Series_title_3,
    test_perf_model_2.Industry_name_NZSIOC AS Industry_name_NZSIOC,
    test_perf_model_2.service_label_1 AS service_label_1,
    test_perf_model_2.Series_reference AS Series_reference,
    test_perf_model_2.Variable_name_1 AS Variable_name_1,
    test_perf_model_2.Group_1 AS Group_1,
    test_perf_model_2.Series_title_4 AS Series_title_4,
    test_perf_model_2.Value_1 AS Value_1,
    test_perf_model_2.Year_1 AS Year_1,
    test_perf_model_2.Industry_code_NZSIOC AS Industry_code_NZSIOC,
    test_perf_model_2.Series_title_2 AS Series_title_2,
    test_perf_model_2.Period_1 AS Period_1,
    test_perf_model_2.Variable_category_1 AS Variable_category_1,
    test_perf_model_2.Series_title_1 AS Series_title_1,
    test_perf_model_2.Industry_aggregation_NZSIOC AS Industry_aggregation_NZSIOC,
    test_perf_model_2.Suppressed_1 AS Suppressed_1,
    test_perf_model_2.Series_title_5 AS Series_title_5,
    test_perf_model_2.country_code AS country_code,
    test_perf_model_2.STATUS_1 AS STATUS_1,
    test_perf_model_2.code_1 AS code_1,
    test_perf_model_2.Magnitude_1 AS Magnitude_1,
    test_perf_model_2.Variable_code_1 AS Variable_code_1,
    test_perf_model_2.country_label AS country_label,
    test_perf_model_2.Industry_code_ANZSIC06 AS Industry_code_ANZSIC06
  
  FROM test_perf_model_2
  INNER JOIN test_perf_model_0 AS annual_enterprise_perf
     ON test_perf_model_2.UNITS_1 = annual_enterprise_perf.UNITS_1

),

all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_performance_database', 'all_type_non_partitioned') }}

),

test_perf_model_1_1 AS (

  SELECT * 
  
  FROM test_perf_model_1

),

Join_3 AS (

  SELECT 
    test_perf_model_1_1.city AS city,
    test_perf_model_1_1.state AS state,
    test_perf_model_1_1.pin AS pin,
    test_perf_model_1_1.p_int AS p_int,
    test_perf_model_1_1.p_string AS p_string,
    all_type_non_partitioned.c_string AS c_string,
    all_type_non_partitioned.c_int AS c_int,
    all_type_non_partitioned.c_bigint AS c_bigint,
    all_type_non_partitioned.c_smallint AS c_smallint,
    all_type_non_partitioned.c_tinyint AS c_tinyint,
    all_type_non_partitioned.c_float AS c_float,
    all_type_non_partitioned.c_boolean AS c_boolean,
    all_type_non_partitioned.c_array AS c_array,
    all_type_non_partitioned.c_double AS c_double,
    all_type_non_partitioned.c_struct AS c_struct
  
  FROM all_type_non_partitioned
  INNER JOIN test_perf_model_1_1
     ON all_type_non_partitioned.c_tinyint = test_perf_model_1_1.c_tinyint AND all_type_non_partitioned.c_smallint = test_perf_model_1_1.c_smallint AND all_type_non_partitioned.c_int = test_perf_model_1_1.c_int AND all_type_non_partitioned.c_bigint = test_perf_model_1_1.c_bigint AND all_type_non_partitioned.c_float = test_perf_model_1_1.c_float AND all_type_non_partitioned.c_double = test_perf_model_1_1.c_double AND all_type_non_partitioned.c_string = test_perf_model_1_1.c_string AND all_type_non_partitioned.c_boolean = test_perf_model_1_1.c_boolean

),

Limit_1_1 AS (

  SELECT * 
  
  FROM Join_3 AS in0
  
  LIMIT 10

),

Limit_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  LIMIT 10

),

Reformat_1 AS (

  SELECT * 
  
  FROM Limit_1 AS in0

),

Macro_1 AS (

  {{ Perf_SQL_Databricks.qa_all_null_base(model = 'Join_2', column_name = 'c_string') }}

),

SQLStatement_1 AS (

  SELECT 
    f1.Series_title_1,
    count(m1.c_tinyint) AS c_tinyint_count,
    count(m1.c_smallint) AS c_small_int_count
  
  FROM Macro_1 AS m1, Filter_1 AS f1
  
  WHERE f1.p_string != NULL
  
  GROUP BY 
    m1.c_boolean, f1.Series_title_1
  
  HAVING c_small_int_count > 0
  
  LIMIT 20

),

Join_4 AS (

  SELECT 
    in2.city AS city,
    in2.state AS state,
    in2.pin AS pin,
    in2.p_int AS p_int,
    in2.p_string AS p_string,
    in2.c_string AS c_string,
    in2.c_int AS c_int,
    in2.c_bigint AS c_bigint,
    in2.c_smallint AS c_smallint,
    in2.c_tinyint AS c_tinyint,
    in2.c_float AS c_float,
    in2.c_boolean AS c_boolean,
    in2.c_array AS c_array,
    in2.c_double AS c_double,
    in2.c_struct AS c_struct
  
  FROM SQLStatement_1 AS in0
  RIGHT JOIN Reformat_1 AS in1
     ON in0.Series_title_1 != in1.Units_1
  LEFT JOIN Limit_1_1 AS in2
     ON in1.Units_1 != in2.c_string

)

SELECT *

FROM Join_4
