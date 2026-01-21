/*
Name:                test_new_env_variable_for_secure_schema
APPID-Name:          N/A
Purpose:             We added an environment variable to the Merch Analytics repo to allow access to the dev and prod schemas maintained by Analytics Engineering for use with Clarity data
Variable(s):         {{dsa_ai_secure_schema}} PROTO_DSA_AI_BASE_VWS for dev or PRD_NAP_DSA_AI_BASE_VWS for prod
Author(s):           Jevon Barlas
Date Created:        9/3/2024
Date Last Updated:   9/3/2024
*/

-- test insert of data into BASE_VWS schema
insert into {dsa_ai_secure_schema}.SOURCE_OF_GOODS_INFLOWS_FACT

   select
      '12345678' as rms_sku_num
      ,110 as channel_num
      ,cast('2024-08-12' as date) as day_date
      ,0 as received_units
      ,0 as po_transfer_in_units             
      ,0 as reserve_stock_transfer_in_units  
      ,0 as pack_and_hold_transfer_in_units  
      ,0 as racking_transfer_in_units        
      ,current_timestamp as dw_sys_load_tmstp

   from prd_nap_base_vws.day_cal_454_dim

   where day_idnt = 2024010
   
;

-- test selection of data from USR_VWS schema
select *
from {dsa_ai_secure_schema}.SOURCE_OF_GOODS_INFLOWS_FACT
;

-- test deletion of data from BASE_VWS schema
delete from {dsa_ai_secure_schema}.SOURCE_OF_GOODS_INFLOWS_FACT
;
