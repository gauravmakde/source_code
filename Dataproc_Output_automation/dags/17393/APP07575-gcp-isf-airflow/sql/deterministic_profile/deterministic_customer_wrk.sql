/*
SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_dim_17393_customer_das_customer;
Task_Name=deterministic_customer_wrk;'
FOR SESSION VOLATILE;
*/

-----------------------------------------------------------------------------
----------------------  STG.DETERMINISTIC_CUSTOMER_WRK ----------------------
-----------------------------------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_wrk{{params.tbl_sfx}} where TRUE;
--PRD_NAP_BASE_VWS.DETERMINISTIC_CUSTOMER_WRK ;--{tbl_sfx};


-- Load pending deterministic_profile_ids into WRK table
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_wrk{{params.tbl_sfx}} --{tbl_sfx}`
SELECT DISTINCT
    deterministic_profile_id,
    SUBSTR(deterministic_profile_id, 1, STRPOS(deterministic_profile_id, '::') - 1) AS customer_source,
    profile_event_tmstp_utc,
    profile_event_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_deterministic_customer_dim{{params.tbl_sfx}}--{tbl_sfx}`
WHERE change_flag = 'DML';
 

-- Collect statistics

-----------------------------------------------------------------------------
------------------ STG.DETERMINISTIC_TRAN_PROFILE_LVL2_WRK ------------------
-----------------------------------------------------------------------------


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_tran_profile_lvl2_wrk{{params.tbl_sfx}} WHERE TRUE;
--PRD_NAP_BASE_VWS.DETERMINISTIC_TRAN_PROFILE_LVL2_WRK; --{tbl_sfx}`;

-- Insert calculated data into destination table
-- Calculate net amount and number of visits to FL and Rack stores.
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_tran_profile_lvl2_wrk{{params.tbl_sfx}}		
--`cf-nordstrom`.PRD_NAP_BASE_VWS.DETERMINISTIC_TRAN_PROFILE_LVL2_WRK --{tbl_sfx}
(deterministic_profile_id, store_num, store_typ_code, trip_cnt, net_amt, max_sale_dt)
SELECT
    dcp_wrk.deterministic_profile_id AS deterministic_profile_id,
    ertm_dtl_fct.store_num AS store_num,
    MAX(STORE.STORE_TYPE_CODE) AS store_typ_code,
    COUNT(DISTINCT ertm_dtl_fct.dcp_trip_id) AS trip_cnt,
    SUM(ertm_dtl_fct.line_net_usd_amt) AS net_amt,
    MAX(CASE WHEN ertm_dtl_fct.net_items > 0 THEN ertm_dtl_fct.customer_shopped_date END) AS max_sale_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_wrk{{params.tbl_sfx}} --{tbl_sfx} 
dcp_wrk
JOIN (
    SELECT
        deterministic_profile_id,
        global_tran_id,
        line_item_seq_num,
        line_net_usd_amt,
        net_items,
        store_num,
        business_day_date,
        customer_shopped_date,
        CASE WHEN net_items > 0
            THEN CONCAT(deterministic_profile_id, store_num, customer_shopped_date)
            ELSE NULL
        END AS dcp_trip_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw
) ertm_dtl_fct
ON dcp_wrk.deterministic_profile_id = ertm_dtl_fct.deterministic_profile_id
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim store
ON ertm_dtl_fct.store_num = STORE.STORE_NUM
WHERE STORE.STORE_NUM NOT IN (828)
AND STORE.STORE_NUM NOT IN (
    SELECT DISTINCT store_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim
    WHERE store_abbrev_name LIKE '%CLOSED%' OR store_abbrev_name LIKE '%E-ST%'
)
AND STORE.STORE_TYPE_CODE IN ('RK', 'FL')
GROUP BY dcp_wrk.deterministic_profile_id, ertm_dtl_fct.store_num;

-- Collect statistics
--COLLECT STATS ON {db_env}_NAP_STG.DETERMINISTIC_TRAN_PROFILE_LVL2_WRK{tbl_sfx};

-----------------------------------------------------------------------------
--------------- STG.DETERMINISTIC_CUSTOMER_STOREOFLOYALTY_WRK ---------------
-----------------------------------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_storeofloyalty_wrk{{params.tbl_sfx}} WHERE TRUE
--PRD_NAP_BASE_VWS.DETERMINISTIC_CUSTOMER_STOREOFLOYALTY_WRK --{tbl_sfx}`
;

-- Calculate customer's store visit for FLS and Rack separately.
-- If there is tie between the two stores then use highest spend as tie breaker.

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_storeofloyalty_wrk{{params.tbl_sfx}}
--PRD_NAP_BASE_VWS.DETERMINISTIC_CUSTOMER_STOREOFLOYALTY_WRK  --{tbl_sfx}`
(deterministic_profile_id, freq_purch_store_id, fls_freq_purch_store_id, rack_freq_purch_store_id)
SELECT
    inn.deterministic_profile_id,
    MAX(CASE WHEN inn.rnk_trips = 1 THEN inn.store_num ELSE NULL END) AS freq_purch_store_id,
    MAX(CASE WHEN inn.rnk_stor_typ_trips = 1 AND inn.store_typ_code = 'FL' THEN inn.store_num ELSE NULL END) AS fls_freq_purch_store_id,
    MAX(CASE WHEN inn.rnk_stor_typ_trips = 1 AND inn.store_typ_code = 'RK' THEN inn.store_num ELSE NULL END) AS rack_freq_purch_store_id
			 
FROM (
    SELECT
        deterministic_profile_id,
        store_typ_code,
        net_amt,
        trip_cnt,
        store_num,
        ROW_NUMBER() OVER(PARTITION BY deterministic_profile_id ORDER BY trip_cnt DESC, net_amt DESC, max_sale_dt DESC) AS rnk_trips,
        ROW_NUMBER() OVER(PARTITION BY deterministic_profile_id, store_typ_code ORDER BY trip_cnt DESC, net_amt DESC, max_sale_dt DESC) AS rnk_stor_typ_trips
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_tran_profile_lvl2_wrk{{params.tbl_sfx}}  --{tbl_sfx}`
    -- QUALIFY clause is not supported in BigQuery; use WHERE instead after defining the rows
) inn
WHERE inn.rnk_trips = 1 OR inn.rnk_stor_typ_trips = 1
GROUP BY inn.deterministic_profile_id;
 

-- Collect statistics
--COLLECT STATS ON {db_env}_NAP_STG.DETERMINISTIC_CUSTOMER_STOREOFLOYALTY_WRK{tbl_sfx};

-----------------------------------------------------------------------------
----------------------- STG.CUSTOMER_PROGRAM_INDEX_WRK ----------------------
-----------------------------------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_program_index_wrk{{params.tbl_sfx}} WHERE TRUE --PRD_NAP_BASE_VWS.CUSTOMER_PROGRAM_INDEX_WRK --{tbl_sfx}`
;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_program_index_wrk{{params.tbl_sfx}}--PRD_NAP_BASE_VWS.CUSTOMER_PROGRAM_INDEX_WRK --{tbl_sfx}`
SELECT
    unique_source_id,
    shopper_id,
    ocp_id,
    loyalty_id
FROM (
 
    SELECT
        unique_source_id,
        program_index_id,
        program_index_name
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_program_dim{{params.tbl_sfx}} --{tbl_sfx}`
    WHERE program_index_name IN ('WEB','OCP','MTLYLTY')
) src
PIVOT (
 
    MAX(program_index_id)
    FOR program_index_name IN ('WEB' AS shopper_id, 'OCP' AS ocp_id, 'MTLYLTY' AS loyalty_id)
);
						
-- Collect statistics
--COLLECT STATS ON {db_env}_NAP_STG.CUSTOMER_PROGRAM_INDEX_WRK{tbl_sfx};

-----------------------------------------------------------------------------
---------------------- STG.CUSTOMER_LATEST_ADDRESS_WRK ----------------------
-----------------------------------------------------------------------------
--CUSTECO-9048

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_latest_address_wrk{{params.tbl_sfx}} WHERE TRUE
--PRD_NAP_BASE_VWS.CUSTOMER_LATEST_ADDRESS_WRK --{tbl_sfx}`
;

-- Insert latest customer address data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_latest_address_wrk{{params.tbl_sfx}}
														
--PRD_NAP_BASE_VWS.CUSTOMER_LATEST_ADDRESS_WRK --{tbl_sfx}`
SELECT
    postal.unique_source_id,
    postal.country_code,
    postal.postal_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_aux_dim{{params.tbl_sfx}} --{tbl_sfx}` 
postal
WHERE postal.country_code IN ('US', 'CA')
																																	
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY postal.unique_source_id
    ORDER BY 
        CASE 
            WHEN postal.address_types LIKE '%DEFAULTBILLING%' THEN 1
            WHEN postal.address_types LIKE '%BILLING%' THEN 2
            WHEN postal.address_types LIKE '%DEFAULTSHIPPING%' THEN 3
            WHEN postal.address_types LIKE '%SHIPPING%' THEN 4
            WHEN postal.address_types LIKE '%WISHLIST%' THEN 5
            ELSE 6 
        END ASC,
        postal.source_audit_update_tmstp DESC,
        postal.object_event_tmstp DESC,
        postal_code DESC
) = 1;

-- Collect statistics
--COLLECT STATS ON {db_env}_NAP_STG.CUSTOMER_LATEST_ADDRESS_WRK{tbl_sfx};

-- SET QUERY_BAND = NONE FOR SESSION;
