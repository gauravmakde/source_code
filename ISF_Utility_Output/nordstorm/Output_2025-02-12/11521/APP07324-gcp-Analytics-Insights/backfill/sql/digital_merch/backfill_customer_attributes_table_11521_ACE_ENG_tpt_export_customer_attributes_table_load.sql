create or replace temporary view data_view 
        (
     acp_id                  string
    ,session_id              string
    ,channel                 string
    ,platform                string
    ,loyalty_status           string     
    ,cardmember_flag          integer     
    ,member_flag              integer    
    ,engagement_cohort        string     
    ,billing_zipcode          string     
    ,us_dma_code              integer     
    ,dma_desc                 string     
    ,CITY_NAME                string     
    ,STATE_NAME               string     
    ,predicted_ct_segment     string     
    ,clv_jwn                  numeric(14,4)      
    ,clv_op                   numeric(14,4)        
    ,clv_fp                  numeric(14,4)        
    ,aare_chnl_code          string      
    ,aare_status_date        date      
    ,activated_chnl_code     string      
    ,activated_date          date
    ,ncom_first_digital_transaction     date
    ,ncom_second_digital_transaction    date
    ,ncom_last_digital_transaction      date
    ,rcom_first_digital_transaction     date
    ,rcom_second_digital_transaction        date
    ,rcom_last_digital_transaction      date
    ,first_digital_transaction          date
    ,second_digital_transaction         date
    ,last_digital_transaction           date
    ,pre_22_digital_order_count                integer
    ,pre_22_digital_demand                 numeric(32,4) 
    ,pre_22_digital_units                  integer
    ,pre_22_ncom_order_count               integer
    ,pre_22_ncom_demand                    numeric(32,4) 
    ,pre_22_ncom_units                     integer
    ,pre_22_rcom_order_count               integer
    ,pre_22_rcom_demand                    numeric(32,4) 
    ,pre_22_rcom_units                 integer
    ,age_group                          string
    ,lifestage                          string
    ,dw_sys_load_tmstp             timestamp
    ,activity_date_pacific                      date
        )
USING csv 
OPTIONS (path "s3://ace-etl/tpt_export/customer_attributes_table/*",
        sep ",",
        header "false",
        dateFormat "yy/M/d",
        escape "");

-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('acedev_etl', 'customer_attributes_table', OUT_RETURN_MSG);
create table if not exists ace_etl.customer_attributes_table
(
	acp_id                  string
    ,session_id             string
    ,channel                string
    ,platform               string
    ,loyalty_status           string     
    ,cardmember_flag          integer     
    ,member_flag              integer    
    ,engagement_cohort        string     
    ,billing_zipcode          string     
    ,us_dma_code              integer     
    ,dma_desc                 string     
    ,CITY_NAME                string     
    ,STATE_NAME               string     
    ,predicted_ct_segment     string     
    ,clv_jwn                  numeric(14,4)      
    ,clv_op                   numeric(14,4)        
    ,clv_fp                  numeric(14,4)        
    ,aare_chnl_code          string      
    ,aare_status_date        date      
    ,activated_chnl_code     string      
    ,activated_date          date
    ,ncom_first_digital_transaction     date
    ,ncom_second_digital_transaction    date
    ,ncom_last_digital_transaction      date
    ,rcom_first_digital_transaction     date
    ,rcom_second_digital_transaction        date
    ,rcom_last_digital_transaction      date
    ,first_digital_transaction          date
    ,second_digital_transaction         date
    ,last_digital_transaction           date
    ,pre_22_digital_order_count                integer
    ,pre_22_digital_demand                 numeric(32,4) 
    ,pre_22_digital_units                  integer
    ,pre_22_ncom_order_count               integer
    ,pre_22_ncom_demand                    numeric(32,4) 
    ,pre_22_ncom_units                     integer
    ,pre_22_rcom_order_count               integer
    ,pre_22_rcom_demand                    numeric(32,4) 
    ,pre_22_rcom_units                 integer
    ,age_group                          string
    ,lifestage                          string
    ,dw_sys_load_tmstp             timestamp
    ,activity_date_pacific                      date
        )
USING PARQUET
location 's3://ace-etl/customer_attributes_table/'
partitioned by (activity_date_pacific);


--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table ace_etl.customer_attributes_table;

insert overwrite table ace_etl.customer_attributes_table partition (activity_date_pacific)
select
     acp_id
    ,session_id
    ,channel 
    ,platform 
    ,loyalty_status                
    ,cardmember_flag               
    ,member_flag                  
    ,engagement_cohort             
    ,billing_zipcode               
    ,us_dma_code                   
    ,dma_desc                      
    ,CITY_NAME                     
    ,STATE_NAME                    
    ,predicted_ct_segment          
    ,clv_jwn                  
    ,clv_op                   
    ,clv_fp                  
    ,aare_chnl_code         
    ,aare_status_date       
    ,activated_chnl_code           
    ,activated_date     
    ,ncom_first_digital_transaction
    ,ncom_second_digital_transaction
    ,ncom_last_digital_transaction
    ,rcom_first_digital_transaction
    ,rcom_second_digital_transaction
    ,rcom_last_digital_transaction
    ,first_digital_transaction
    ,second_digital_transaction
    ,last_digital_transaction    
    ,pre_22_digital_order_count
    ,pre_22_digital_demand
    ,pre_22_digital_units
    ,pre_22_ncom_order_count
    ,pre_22_ncom_demand
    ,pre_22_ncom_units
    ,pre_22_rcom_order_count
    ,pre_22_rcom_demand
    ,pre_22_rcom_units
    ,age_group                     
    ,lifestage                  
    ,dw_sys_load_tmstp             
    ,activity_date_pacific                     
from data_view;

