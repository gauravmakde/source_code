SELECT *
FROM (
    SELECT
        CAST('week_start_day_date' AS VARCHAR(1000)) as week_start_day_date,  
        CAST('note_type' AS VARCHAR(1000)) as note_type,  
        CAST('redemption_business_unit_desc' AS VARCHAR(1000)) as redemption_business_unit_desc,  
        CAST('redeemed_next_30days' AS VARCHAR(1000)) as redeemed_next_30days,  
        CAST('redeemed_next_60days' AS VARCHAR(1000)) as redeemed_next_60days,  
        CAST('redeemed_next_90days' AS VARCHAR(1000)) as redeemed_next_90days,  
        CAST('redeemed_next_120days' AS VARCHAR(1000)) as redeemed_next_120days,  
        CAST('note_more120' AS VARCHAR(1000)) as note_more120,  
        CAST('not_redeemed' AS VARCHAR(1000)) as not_redeemed 
    FROM ( SELECT 1 as one) dummy

    UNION ALL

    SELECT 
      TO_CHAR(week_start_day_date, 'YYYY-MM-DD'),                     
      note_type,                               
      redemption_business_unit_desc,           
      TRIM(redeemed_next_30days(VARCHAR(100))) as redeemed_next_30days,                    
      TRIM(redeemed_next_60days(VARCHAR(100))) as redeemed_next_60days,                    
      TRIM(redeemed_next_90days(VARCHAR(100))) as redeemed_next_90days,                    
      TRIM(redeemed_next_120days(VARCHAR(100))) as redeemed_next_120days,                   
      TRIM(note_more120(VARCHAR(100))) as note_more120,                            
      TRIM(not_redeemed(VARCHAR(100))) as not_redeemed
    FROM {proto_schema}.MMM_LOYALTY_REDEMPTION_LDG
)rsltset
ORDER BY CASE WHEN week_start_day_date = ''week_start_day_date'' THEN 1 ELSE 2 END;
