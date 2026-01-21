select
	event_id 
    ,event_name        
    ,featured_flag 
    ,private_sale_flag 
    ,unique_brands      
    ,branded_flag       
    ,brand_name 
    ,event_start         
    ,event_end  
    ,dw_sys_load_tmstp   
from {digital_merch_t2_schema}.flash_event_output
; 