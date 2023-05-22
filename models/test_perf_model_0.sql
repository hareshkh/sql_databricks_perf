WITH annual_enterprise_perf AS (

  SELECT * 
  
  FROM {{ ref('annual_enterprise_perf')}}

)

SELECT *

FROM annual_enterprise_perf
