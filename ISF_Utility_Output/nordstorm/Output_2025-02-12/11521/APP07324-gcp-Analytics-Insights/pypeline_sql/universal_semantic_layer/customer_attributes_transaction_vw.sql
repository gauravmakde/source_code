SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=customer_attributes_transaction_vw_11521_ACE_ENG;
     Task_Name=customer_attributes_transaction_vw;'
     FOR SESSION VOLATILE;

/*


T2/View Name:t2dl_das_usl.customer_attributes_transaction_vw
Team/Owner:Customer Analytics
Date Created/Modified:2023-05-10

Note:
-- Customer-level transaction based attribute view

*/

REPLACE VIEW {usl_t2_schema}.customer_attributes_transaction_vw
AS
LOCK ROW FOR ACCESS

	 select cus.acp_id 
            ,case
                    when active_1year_nstores >0 AND active_1year_ncom = 0 AND active_1year_rstores = 0 AND active_1year_rcom = 0 then 'nstore only shopper'
                    when active_1year_nstores =0 AND active_1year_ncom > 0 AND active_1year_rstores = 0 AND active_1year_rcom = 0 then 'ncom only holiday shopper'
                    when active_1year_nstores =0 AND active_1year_ncom = 0 AND active_1year_rstores > 0 AND active_1year_rcom = 0 then 'rstore only holiday shopper'
                    when active_1year_nstores =0 AND active_1year_ncom = 0 AND active_1year_rstores = 0 AND active_1year_rcom > 0 then 'rcom only holiday shopper'

                    when active_1year_nstores =0 AND active_1year_ncom = 0 AND active_1year_rstores > 0 AND active_1year_rcom > 0 then '2box rack banner shopper'
                    when active_1year_nstores >0 AND active_1year_ncom > 0 AND active_1year_rstores = 0 AND active_1year_rcom = 0 then '2box nordstrom banner shopper'
                    when active_1year_nstores =0 AND active_1year_ncom > 0 AND active_1year_rstores = 0 AND active_1year_rcom > 0 then '2box digital only shopper'
                    when active_1year_nstores >0 AND active_1year_ncom = 0 AND active_1year_rstores > 0 AND active_1year_rcom = 0 then '2box store only shopper'

                    when active_1year_nstores + active_1year_ncom + active_1year_rstores + active_1year_rcom = 2 then '2box crossbox shopper'
                    when active_1year_nstores + active_1year_ncom + active_1year_rstores + active_1year_rcom = 3 then '3box shopper'
                    when active_1year_nstores + active_1year_ncom + active_1year_rstores + active_1year_rcom = 4 then '4box shopper'
                    when active_1year_nstores + active_1year_ncom + active_1year_rstores + active_1year_rcom = 0 then 'dormant shopper'
                    else 'other'
                    end AS customer_boxes
            
            ,case
                    when shopped_ly_holiday_nstores >0 AND shopped_ly_holiday_ncom = 0 AND shopped_ly_holiday_rstores = 0 AND shopped_ly_holiday_rcom = 0 then 'nstore only holiday shopper'
                    when shopped_ly_holiday_nstores =0 AND shopped_ly_holiday_ncom > 0 AND shopped_ly_holiday_rstores = 0 AND shopped_ly_holiday_rcom = 0 then 'ncom only holiday shopper'
                    when shopped_ly_holiday_nstores =0 AND shopped_ly_holiday_ncom = 0 AND shopped_ly_holiday_rstores > 0 AND shopped_ly_holiday_rcom = 0 then 'rstore only holiday shopper'
                    when shopped_ly_holiday_nstores =0 AND shopped_ly_holiday_ncom = 0 AND shopped_ly_holiday_rstores = 0 AND shopped_ly_holiday_rcom > 0 then 'rcom only holiday shopper'

                    when shopped_ly_holiday_nstores =0 AND shopped_ly_holiday_ncom = 0 AND shopped_ly_holiday_rstores > 0 AND shopped_ly_holiday_rcom > 0 then '2box rack only holiday shopper'
                    when shopped_ly_holiday_nstores >0 AND shopped_ly_holiday_ncom > 0 AND shopped_ly_holiday_rstores = 0 AND shopped_ly_holiday_rcom = 0 then '2box nord only holiday shopper'
                    when shopped_ly_holiday_nstores =0 AND shopped_ly_holiday_ncom > 0 AND shopped_ly_holiday_rstores = 0 AND shopped_ly_holiday_rcom > 0 then '2box digital only holiday shopper'
                    when shopped_ly_holiday_nstores >0 AND shopped_ly_holiday_ncom = 0 AND shopped_ly_holiday_rstores > 0 AND shopped_ly_holiday_rcom = 0 then '2box store only holiday shopper'

                    when shopped_ly_holiday_nstores + shopped_ly_holiday_ncom + shopped_ly_holiday_rstores + shopped_ly_holiday_rcom = 2 then '2box crossbox holiday shopper'
                    when shopped_ly_holiday_nstores + shopped_ly_holiday_ncom + shopped_ly_holiday_rstores + shopped_ly_holiday_rcom = 3 then '3box holiday shopper'
                    when shopped_ly_holiday_nstores + shopped_ly_holiday_ncom + shopped_ly_holiday_rstores + shopped_ly_holiday_rcom = 4 then '4box holiday shopper'
                    when shopped_ly_holiday_nstores + shopped_ly_holiday_ncom + shopped_ly_holiday_rstores + shopped_ly_holiday_rcom = 0 then 'non holiday shopper'
                    else 'other'
                    end AS customer_boxes_holiday

            ,case
                    when coalesce(return_rate_spend_1yr_nstores,0) =0  then 'return quartile 0'
                    when return_rate_spend_1yr_nstores >0.00 and return_rate_spend_1yr_nstores<=0.25 then 'return quartile 1'
                    when return_rate_spend_1yr_nstores >0.25 and return_rate_spend_1yr_nstores<=0.50 then 'return quartile 2'
                    when return_rate_spend_1yr_nstores >0.50 and return_rate_spend_1yr_nstores<=0.75 then 'return quartile 3'
                    when return_rate_spend_1yr_nstores >0.75 and return_rate_spend_1yr_nstores<=1.00 then 'return quartile 4'
                    end customer_return_quartile_nstores
            ,case
                    when coalesce(return_rate_spend_1yr_ncom,0) =0 then 'return quartile 0'
                    when return_rate_spend_1yr_ncom >0.00 and return_rate_spend_1yr_ncom<=0.25 then 'return quartile 1'
                    when return_rate_spend_1yr_ncom >0.25 and return_rate_spend_1yr_ncom<=0.50 then 'return quartile 2'
                    when return_rate_spend_1yr_ncom >0.50 and return_rate_spend_1yr_ncom<=0.75 then 'return quartile 3'
                    when return_rate_spend_1yr_ncom >0.75 and return_rate_spend_1yr_ncom<=1.00 then 'return quartile 4'
                    end customer_return_quartile_ncom
            ,case
                    when coalesce(return_rate_spend_1yr_rstores,0) =0 then 'return quartile 0'
                    when return_rate_spend_1yr_rstores >0.00 and return_rate_spend_1yr_rstores<=0.25 then 'return quartile 1'
                    when return_rate_spend_1yr_rstores >0.25 and return_rate_spend_1yr_rstores<=0.50 then 'return quartile 2'
                    when return_rate_spend_1yr_rstores >0.50 and return_rate_spend_1yr_rstores<=0.75 then 'return quartile 3'
                    when return_rate_spend_1yr_rstores >0.75 and return_rate_spend_1yr_rstores<=1.00 then 'return quartile 4'
                    end customer_return_quartile_rstores
            ,case
                    when coalesce(return_rate_spend_1yr_rcom,0) =0 then 'return quartile 0'
                    when return_rate_spend_1yr_rcom >0.00 and return_rate_spend_1yr_rcom<=0.25 then 'return quartile 1'
                    when return_rate_spend_1yr_rcom >0.25 and return_rate_spend_1yr_rcom<=0.50 then 'return quartile 2'
                    when return_rate_spend_1yr_rcom >0.50 and return_rate_spend_1yr_rcom<=0.75 then 'return quartile 3'
                    when return_rate_spend_1yr_rcom >0.75 and return_rate_spend_1yr_rcom<=1.00 then 'return quartile 4'
                    end customer_return_quartile_rcom

            ,rfm_1year_segment customer_rfm
            ,rfm_4year_segment customer_rfm_4yr
	
	from T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS cus
        join {usl_t2_schema}.acp_driver_dim ac on ac.acp_id = cus.acp_id
;

SET QUERY_BAND = NONE FOR SESSION;