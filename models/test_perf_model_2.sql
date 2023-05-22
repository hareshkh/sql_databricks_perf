WITH ecards_transactions_perf AS (

  SELECT * 
  
  FROM {{ ref('ecards_transactions_perf')}}

),

annual_enterprise_perf AS (

  SELECT * 
  
  FROM {{ ref('annual_enterprise_perf')}}

),

Join_1 AS (

  SELECT 
    ecards_transactions_perf.Series_reference AS Series_reference,
    ecards_transactions_perf.Period_1 AS Period_1,
    ecards_transactions_perf.Data_value_1 AS Data_value_1,
    ecards_transactions_perf.Suppressed_1 AS Suppressed_1,
    ecards_transactions_perf.STATUS_1 AS STATUS_1,
    ecards_transactions_perf.UNITS_1 AS UNITS_1,
    ecards_transactions_perf.Magnitude_1 AS Magnitude_1,
    ecards_transactions_perf.Subject_1 AS Subject_1,
    ecards_transactions_perf.Group_1 AS Group_1,
    ecards_transactions_perf.Series_title_1 AS Series_title_1,
    ecards_transactions_perf.Series_title_2 AS Series_title_2,
    ecards_transactions_perf.Series_title_3 AS Series_title_3,
    ecards_transactions_perf.Series_title_4 AS Series_title_4,
    ecards_transactions_perf.Series_title_5 AS Series_title_5,
    annual_enterprise_perf.Variable_category_1 AS Variable_category_1,
    annual_enterprise_perf.Industry_code_NZSIOC AS Industry_code_NZSIOC,
    annual_enterprise_perf.Year_1 AS Year_1,
    annual_enterprise_perf.Industry_code_ANZSIC06 AS Industry_code_ANZSIC06,
    annual_enterprise_perf.Variable_name_1 AS Variable_name_1,
    annual_enterprise_perf.Industry_aggregation_NZSIOC AS Industry_aggregation_NZSIOC,
    annual_enterprise_perf.Variable_code_1 AS Variable_code_1,
    annual_enterprise_perf.Value_1 AS Value_1,
    annual_enterprise_perf.Industry_name_NZSIOC AS Industry_name_NZSIOC
  
  FROM annual_enterprise_perf
  INNER JOIN ecards_transactions_perf
     ON annual_enterprise_perf.Units_1 = ecards_transactions_perf.Units_1

),

economic_survey_perf AS (

  SELECT * 
  
  FROM {{ ref('economic_survey_perf')}}

),

ecards_transactions_perf_1 AS (

  SELECT * 
  
  FROM {{ ref('ecards_transactions_perf')}}

),

Join_2 AS (

  SELECT 
    ecards_transactions_perf_1.Data_value_1 AS Data_value_1,
    economic_survey_perf.STATUS_1 AS STATUS_1,
    economic_survey_perf.UNITS_1 AS UNITS_1,
    economic_survey_perf.Series_reference AS Series_reference,
    economic_survey_perf.Series_title_2 AS Series_title_2,
    economic_survey_perf.Magnitude_1 AS Magnitude_1,
    economic_survey_perf.Series_title_5 AS Series_title_5,
    economic_survey_perf.Period_1 AS Period_1,
    economic_survey_perf.Series_title_1 AS Series_title_1,
    economic_survey_perf.Suppressed_1 AS Suppressed_1,
    economic_survey_perf.Subject_1 AS Subject_1,
    economic_survey_perf.Series_title_4 AS Series_title_4,
    economic_survey_perf.Data_value AS Data_value,
    economic_survey_perf.Series_title_3 AS Series_title_3,
    economic_survey_perf.Group_1 AS Group_1
  
  FROM economic_survey_perf
  INNER JOIN ecards_transactions_perf_1
     ON economic_survey_perf.Series_reference = ecards_transactions_perf_1.Series_reference AND economic_survey_perf.Period_1 = ecards_transactions_perf_1.Period_1 AND economic_survey_perf.Suppressed_1 = ecards_transactions_perf_1.Suppressed_1 AND economic_survey_perf.STATUS_1 = ecards_transactions_perf_1.STATUS_1 AND economic_survey_perf.UNITS_1 = ecards_transactions_perf_1.UNITS_1 AND economic_survey_perf.Magnitude_1 = ecards_transactions_perf_1.Magnitude_1 AND economic_survey_perf.Subject_1 = ecards_transactions_perf_1.Subject_1 AND economic_survey_perf.Group_1 = ecards_transactions_perf_1.Group_1 AND economic_survey_perf.Series_title_1 = ecards_transactions_perf_1.Series_title_1 AND economic_survey_perf.Series_title_2 = ecards_transactions_perf_1.Series_title_2 AND economic_survey_perf.Series_title_3 = ecards_transactions_perf_1.Series_title_3 AND economic_survey_perf.Series_title_4 = ecards_transactions_perf_1.Series_title_4 AND economic_survey_perf.Series_title_5 = ecards_transactions_perf_1.Series_title_5

),

service_classification_perf AS (

  SELECT * 
  
  FROM {{ ref('service_classification_perf')}}

),

country_classification_perf AS (

  SELECT * 
  
  FROM {{ ref('country_classification_perf')}}

),

Join_3 AS (

  SELECT 
    in0.code_1 AS code_1,
    in0.service_label_1 AS service_label_1,
    in1.country_code AS country_code,
    in1.country_label AS country_label
  
  FROM service_classification_perf AS in0
  INNER JOIN country_classification_perf AS in1
     ON in0.code_1 != in1.country_code

),

Join_4 AS (

  SELECT 
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
    in0.code_1 AS code_1,
    in0.service_label_1 AS service_label_1,
    in0.country_code AS country_code,
    in0.country_label AS country_label
  
  FROM Join_3 AS in0
  INNER JOIN Join_1 AS in1
     ON in0.code_1 != in1.series_reference
  INNER JOIN Join_2 AS in2
     ON in1.series_reference != in2.STATUS_1

)

SELECT *

FROM Join_4
