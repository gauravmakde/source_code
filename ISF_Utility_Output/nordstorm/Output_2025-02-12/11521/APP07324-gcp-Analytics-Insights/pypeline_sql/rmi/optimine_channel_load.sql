create or replace temporary view optimine_channel_view
(Influencing_Level0  string,
      Influencing_Level1 string ,
      Influencing_Level2 string ,
      Influencing_Level3 string ,
      Influencing_Level4 string ,
      Influencing_Level5 string ,
      Influencing_Level6 string ,
      Influencing_Level7 string ,
      Influencing_Level8 string ,
      Influencing_Level9 string ,
      Converting_Level0 string ,
      Converting_Level1 string ,
      Converting_Level2 string ,
      Converting_Level3 string ,
      Converting_Level4 string ,
      Converting_Level5 string ,
      Converting_Level6 string ,
      Converting_Level7 string ,
      Converting_Level8 string ,
      Converting_Level9 string ,
      TimePeriod string,
      XChannelValue1 string ,
      XChannelValue2 string,
      XChannelValue3  string,
      XChannelValue4  string,
      XChannelValue5  string,
      XChannelCount1 string,
      XChannelCount2 string ,
      XChannelCount3 string ,
      XChannelCount4  string,
      XChannelCount5 string ,
      XChannelCount6 string ,
      XChannelCount7 string ,
      XChannelCount8  string,
      XChannelCount9  string,
      XChannelCount10 string   	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/RMI/*XChannel_*.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE optimine_channel_ldg_output
SELECT
Influencing_Level0  ,
      Influencing_Level1  ,
      Influencing_Level2  ,
      Influencing_Level3  ,
      Influencing_Level4  ,
      Influencing_Level5  ,
      Influencing_Level6  ,
      Influencing_Level7  ,
      Influencing_Level8  ,
      Influencing_Level9  ,
      Converting_Level0  ,
      Converting_Level1  ,
      Converting_Level2  ,
      Converting_Level3  ,
      Converting_Level4  ,
      Converting_Level5  ,
      Converting_Level6  ,
      Converting_Level7  ,
      Converting_Level8  ,
      Converting_Level9  ,
      TimePeriod ,
      XChannelValue1  ,
      XChannelValue2 ,
      XChannelValue3  ,
      XChannelValue4  ,
      XChannelValue5  ,
      XChannelCount1 ,
      XChannelCount2  ,
      XChannelCount3  ,
      XChannelCount4  ,
      XChannelCount5  ,
      XChannelCount6  ,
      XChannelCount7  ,
      XChannelCount8  ,
      XChannelCount9  ,
      XChannelCount10 
FROM optimine_channel_view
WHERE 1=1
;

