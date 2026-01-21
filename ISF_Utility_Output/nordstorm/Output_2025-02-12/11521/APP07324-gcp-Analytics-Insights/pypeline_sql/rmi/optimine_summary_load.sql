create or replace temporary view optimine_summary_view

(Level0 string,
      Level1 string,
      Level2 string,
      Level3 string,
      Level4 string,
      Level5 string,
      Level6 string,
      Level7 string,
      Level8 string,
      Level9 string,
      TimePeriod string,
      ReportedAudience string,
      ReportedCost string,
      ModeledValue1 string,
      ModeledValue2 string,
      ModeledValue3 string,
      ModeledValue4 string,
      ModeledValue5 string,
      ModeledCount1 string,
      ModeledCount2 string,
      ModeledCount3 string,
      ModeledCount4 string,
      ModeledCount5 string,
      ModeledCount6 string,
      ModeledCount7 string,
      ModeledCount8 string,
      ModeledCount9 string,
      ModeledCount10 string     	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/RMI/OptimineSummary_*.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE optimine_summary_ldg_output
SELECT
Level0,
      Level1,
      Level2,
      Level3,
      Level4,
      Level5,
      Level6,
      Level7,
      Level8,
      Level9,
      TimePeriod,
      ReportedAudience,
      ReportedCost,
      ModeledValue1,
      ModeledValue2,
      ModeledValue3,
      ModeledValue4,
      ModeledValue5,
      ModeledCount1,
      ModeledCount2,
      ModeledCount3,
      ModeledCount4,
      ModeledCount5,
      ModeledCount6,
      ModeledCount7,
      ModeledCount8,
      ModeledCount9,
      ModeledCount10    
FROM optimine_summary_view
WHERE 1=1
;

