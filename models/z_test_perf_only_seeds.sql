WITH bos2021model_perf AS (

  SELECT * 
  
  FROM {{ ref('bos2021model_perf')}}

),

business_financial_data_perf AS (

  SELECT * 
  
  FROM {{ ref('business_financial_data_perf')}}

),

service_classification_perf AS (

  SELECT * 
  
  FROM {{ ref('service_classification_perf')}}

),

epl_data_perf AS (

  SELECT * 
  
  FROM {{ ref('epl_data_perf')}}

),

annual_enterprise_perf AS (

  SELECT * 
  
  FROM {{ ref('annual_enterprise_perf')}}

),

business_employee_perf AS (

  SELECT * 
  
  FROM {{ ref('business_employee_perf')}}

),

revised_perf AS (

  SELECT * 
  
  FROM {{ ref('revised_perf')}}

),

economic_survey_perf AS (

  SELECT * 
  
  FROM {{ ref('economic_survey_perf')}}

),

Join_1_1 AS (

  SELECT 
    in0.description_1 AS description_1,
    in0.industry_1 AS industry_1,
    in0.level_1 AS level_1,
    in0.size_1 AS size_1,
    in0.line_code AS line_code,
    in0.value_1 AS value_1
  
  FROM bos2021model_perf AS in0
  INNER JOIN business_employee_perf AS in1
     ON in0.description_1 != in1.Series_reference
  LEFT JOIN business_financial_data_perf AS in2
     ON in1.Series_reference != in2.Series_reference
  RIGHT JOIN economic_survey_perf AS in3
     ON in2.Series_reference != in3.Series_reference
  INNER JOIN revised_perf AS in4
     ON in3.Series_reference != in4.status_1

),

Limit_1_1 AS (

  SELECT * 
  
  FROM Join_1_1 AS in0
  
  LIMIT 10

),

ecards_transactions_perf AS (

  SELECT * 
  
  FROM {{ ref('ecards_transactions_perf')}}

),

country_classification_perf AS (

  SELECT * 
  
  FROM {{ ref('country_classification_perf')}}

),

Join_1 AS (

  SELECT 
    in0.Div_1 AS Div_1,
    in0.Date_1 AS Date_1,
    in0.HomeTeam_1 AS HomeTeam_1,
    in0.AwayTeam_1 AS AwayTeam_1,
    in0.FTHG_1 AS FTHG_1,
    in0.FTAG_1 AS FTAG_1,
    in0.FTR_1 AS FTR_1,
    in0.HTHG_1 AS HTHG_1,
    in0.HTAG_1 AS HTAG_1,
    in0.HTR_1 AS HTR_1,
    in0.Referee_1 AS Referee_1,
    in0.HS_1 AS HS_1,
    in0.AS_1 AS AS_1,
    in0.HST_1 AS HST_1,
    in0.AST_1 AS AST_1,
    in0.HF_1 AS HF_1,
    in0.AF_1 AS AF_1,
    in0.HC_1 AS HC_1,
    in0.AC_1 AS AC_1,
    in0.HY_1 AS HY_1,
    in0.AY_1 AS AY_1,
    in0.HR_1 AS HR_1,
    in0.AR_1 AS AR_1,
    in0.B365H_1 AS B365H_1,
    in0.B365D_1 AS B365D_1,
    in0.B365A_1 AS B365A_1,
    in0.BWH_1 AS BWH_1,
    in0.BWD_1 AS BWD_1,
    in0.BWA_1 AS BWA_1,
    in0.IWH_1 AS IWH_1,
    in0.IWD_1 AS IWD_1,
    in0.IWA_1 AS IWA_1,
    in0.PSH_1 AS PSH_1,
    in0.PSD_1 AS PSD_1,
    in0.PSA_1 AS PSA_1,
    in0.WHH_1 AS WHH_1,
    in0.WHD_1 AS WHD_1,
    in0.WHA_1 AS WHA_1,
    in0.VCH_1 AS VCH_1,
    in0.VCD_1 AS VCD_1,
    in0.VCA_1 AS VCA_1,
    in0.Bb1X2_1 AS Bb1X2_1,
    in0.BbMxH_1 AS BbMxH_1,
    in0.BbAvH_1 AS BbAvH_1,
    in0.BbMxD_1 AS BbMxD_1,
    in0.BbAvD_1 AS BbAvD_1,
    in0.BbMxA_1 AS BbMxA_1,
    in0.BbAvA_1 AS BbAvA_1,
    in0.BbOU_1 AS BbOU_1,
    in0.BbMx_2_5 AS BbMx_2_5,
    in0.BbAv_2_5 AS BbAv_2_5,
    in0.BbAH_1 AS BbAH_1,
    in0.BbMxAHH_1 AS BbMxAHH_1,
    in0.BbAvAHH_1 AS BbAvAHH_1,
    in0.BbMxAHA_1 AS BbMxAHA_1,
    in0.BbAvAHA_1 AS BbAvAHA_1,
    in0.PSCH_1 AS PSCH_1,
    in0.PSCD_1 AS PSCD_1,
    in0.PSCA_1 AS PSCA_1
  
  FROM epl_data_perf AS in0
  INNER JOIN ecards_transactions_perf AS in1
     ON in0.Div_1 != in1.Series_reference
  LEFT JOIN country_classification_perf AS in2
     ON in1.Series_reference != in2.country_code
  RIGHT JOIN service_classification_perf AS in3
     ON in2.country_code != in3.code_1
  INNER JOIN annual_enterprise_perf AS in4
     ON in3.code_1 != in4.Year_1

),

Limit_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  LIMIT 10

),

Join_2 AS (

  SELECT 
    in1.description_1 AS description_1,
    in1.industry_1 AS industry_1,
    in1.level_1 AS level_1,
    in1.size_1 AS size_1,
    in1.line_code AS line_code,
    in1.value_1 AS value_1
  
  FROM Limit_1 AS in0
  INNER JOIN Limit_1_1 AS in1
     ON in0.Div_1 != in1.description_1

)

SELECT *

FROM Join_2
