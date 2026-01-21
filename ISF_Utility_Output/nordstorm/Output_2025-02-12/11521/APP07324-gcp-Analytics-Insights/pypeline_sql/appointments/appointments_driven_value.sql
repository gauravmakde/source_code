/*

APPOINTMENTS_DRIVEN_VALUE.SQL

*/
SET QUERY_BAND = 'App_ID=APP08193;
     DAG_ID=appointments_driven_value_11521_ACE_ENG;
     Task_Name=appointments_driven_value;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: June 18, 2024
*/
    

/*--STEP 1: ROLL TO LATEST BOOKING; UPDATED TO PREFER CUSTOMER OVER ADMINISTRATOR (hm44_blvd_tracking_discovery_booking_refined)---*/
-- Booking Refined Table Creation
-- Exclude Virtual & Cancelled & Focus on Styling Related & Address Rescheduled Bookings issue
--drop table hm44_blvd_tracking_discovery_booking_refined;
CREATE  MULTISET VOLATILE TABLE hm44_blvd_tracking_discovery_booking_refined AS (
	SELECT distinct 
		booking_id, 
		booking_service_id,
		booking_service_description,
		booking_service_name,
		booking_made_by,
		source_store,
		schedule_start_date, 
		SHOPPER_ID, 
		customer_contact_email, customer_contact_phone, customer_contact_first_name, customer_contact_last_name, 
		booking_status,
		client_arrival_time,
		completed_time
	FROM
			(SELECT DISTINCT booking_id
					,max(case when shopper_id <> 'UNKNOWN' 
								AND BOOKING_MADE_BY = 'CUSTOMER' 
								then SHOPPER_ID end) over (partition by booking_id) as SHOPPER_ID	
								
					,CAST(schedule_start_time AS DATE) as schedule_start_date
					--,source_store
					,max(customer_contact_email) over (partition by booking_id) as customer_contact_email
					,max(customer_contact_phone) over (partition by booking_id) as customer_contact_phone
					,max(customer_contact_first_name) over (partition by booking_id) as customer_contact_first_name
					,max(customer_contact_last_name) over (partition by booking_id) as customer_contact_last_name
					
					,booking_made_by
					,source_store
					,booking_service_id
					,booking_service_description
					,booking_service_name
					,booking_status
					,client_arrival_time
					,completed_time
					
					,RANK() OVER (PARTITION BY booking_id ORDER BY event_time DESC) as rank_num 
			
			FROM PRD_NAP_USR_VWS.BOOKING_OBJECT_MODEL_FACT a
			where CAST(schedule_start_time AS DATE) >= '2022-08-07'
                and CAST(schedule_start_time AS DATE) <= current_date()-11
			) core
	--NEW
	WHERE rank_num = 1						--ALWAYS WANT LATEST APPT BOOKING ROW
		and booking_status <> 'CANCELLED'	-- .. of status NOT CANCELLED
		and upper(booking_service_description) not like '%VIRTUAL%'
		and upper(booking_service_name) not like '%VIRTUAL%'
	QUALIFY RANK() OVER (PARTITION BY booking_id ORDER BY booking_status)=1		--WANT RESCHEDULED TO BE PRIORITIZED OVER SCHEDULED FOR ACCURATE SERVICES REPORTING
	
)with data primary index(booking_id) on commit preserve rows;



/*--STEP 2:  SHOPPER_ID TO ACP_ID UPDATE (hm44_blvd_tracking_acp_id)--------*/
--drop table hm44_blvd_tracking_acp_id;
CREATE  MULTISET VOLATILE TABLE hm44_blvd_tracking_acp_id AS (
	with acp_to_shopper_id as
	(
	    select
	        acp_id,
	        program_index_id as shopper_id,
	        row_number() over (partition by shopper_id order by acp_id) as shopper_row_dedupe
	    from
	        prd_nap_usr_vws.acp_analytical_program_xref
	    --where program_name in ('WEB','NRHL')		--deprecated source at this point and represented the IDs from the old Rack.com system
	    where program_name in ('WEB')
	    group by 1,2
	)
	SEL A.*,
		B.ACP_ID
	FROM hm44_blvd_tracking_discovery_booking_refined a
	join acp_to_shopper_id b on a.shopper_id = b.shopper_id 
								and b.shopper_row_dedupe = 1	

)with data primary index(booking_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (booking_id) ON hm44_blvd_tracking_acp_id; 
COLLECT STATISTICS COLUMN (acp_id) ON hm44_blvd_tracking_acp_id; 



/*STEP 3a: Email Match (hm44_blvd_tracking_email_match) -------------------*/
--drop table hm44_blvd_tracking_email_match;
CREATE  MULTISET VOLATILE TABLE hm44_blvd_tracking_email_match AS (

	SELECT DISTINCT a.booking_id, a.schedule_start_date, a.customer_contact_email, c.email_address_token_value as cust_obj_email_token, c.unique_source_id 
	from hm44_blvd_tracking_discovery_booking_refined a
	join prd_nap_usr_vws.CUSTOMER_OBJ_EMAIL_DIM c on a.customer_contact_email = c.email_address_token_value

)with data primary index(booking_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (booking_id) ON hm44_blvd_tracking_email_match; 
COLLECT STATISTICS COLUMN (unique_source_id) ON hm44_blvd_tracking_email_match; 


/*STEP 3b: Phone Match (hm44_blvd_tracking_phone_match) -------------------*/
--drop table hm44_blvd_tracking_phone_match;
CREATE  MULTISET VOLATILE TABLE hm44_blvd_tracking_phone_match AS (
	
	SELECT DISTINCT a.booking_id, 
					a.schedule_start_date, 
					a.customer_contact_phone, 
					--'' as primary_token, 
					--'' as bad_token, 
					c.telephone_number_token_value as cust_obj_telephone_token, 
					c.unique_source_id 
	from hm44_blvd_tracking_discovery_booking_refined a         
	join prd_nap_usr_vws.CUSTOMER_OBJ_TELEPHONE_DIM c	on a.customer_contact_phone = c.telephone_number_token_value

)with data primary index(booking_id) on commit preserve rows;



COLLECT STATISTICS COLUMN (booking_id) ON hm44_blvd_tracking_phone_match; 
COLLECT STATISTICS COLUMN (unique_source_id) ON hm44_blvd_tracking_phone_match; 



--STEP 4: EMAIL & PHONE MATCH HIGH-LEVEL WITH ACP_ID ---ORIGINAL (KEEP UNTIL Q ANSWERED BY TEAMS SUPPORTING)
--DROP TABLE email_phone_match_acp_id;
CREATE multiset volatile TABLE email_phone_match_acp_id AS (
	WITH dat AS (
		SELECT DISTINCT booking_id, schedule_start_date, unique_source_id FROM hm44_blvd_tracking_email_match
		UNION ALL
		SELECT DISTINCT booking_id, schedule_start_date, unique_source_id FROM hm44_blvd_tracking_phone_match
	)
	, with_acp AS (
		SELECT DISTINCT 
			booking_id
			,schedule_start_date
	      ,r1.acp_id
	      ,a.unique_source_id
	      ,r1.dw_sys_updt_tmstp
	FROM DAT a
	JOIN PRD_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF r1 ON STRTOK(a.unique_source_id, '::', 1) = r1.cust_source and STRTOK(a.unique_source_id, '::', 2) = r1.cust_id
	WHERE UPPER(STRTOK(unique_source_id, '::', 1)) IN ('ICON','CHECKOUT')
	)
	sel a.*
		  ,DENSE_RANK() OVER (PARTITION BY booking_id, schedule_start_date 
		  				--PREFER ICON_ID IF AVAILABLE OVER CHECKOUT; IF TIE LOOK FOR LATEST UPDATED
		  				ORDER BY STRTOK(a.unique_source_id, '::', 1) DESC, dw_sys_updt_tmstp DESC) as rank_num
	FROM with_acp a
) with data primary index(booking_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (booking_id) ON email_phone_match_acp_id; 



-- Start using RANK to get only the acp id that's most recently updated for booking 
--DROP TABLE email_phone_match_acp_id_final;
CREATE multiset volatile TABLE email_phone_match_acp_id_final AS (

		SELECT DISTINCT booking_id
		      ,schedule_start_date
		      ,acp_id
		FROM email_phone_match_acp_id
		WHERE rank_num = 1
		QUALIFY DENSE_RANK() OVER (PARTITION BY booking_id, schedule_start_date 
		  				--PREFER ICON_ID IF AVAILABLE OVER CHECKOUT; IF TIE LOOK FOR LATEST UPDATED
		  				ORDER BY ACP_ID DESC)  = 1
		
) with data primary index(booking_id) on commit preserve rows;

COLLECT STATISTICS COLUMN (booking_id) ON email_phone_match_acp_id_final; 




--STEP 5: Filter to most recent leaving only cases where ACP_IDs match (matched_acp_id) -------------------
--drop table matched_acp_id;
CREATE multiset volatile TABLE matched_acp_id AS (
	
	SELECT booking_id, schedule_start_date, acp_id, 'EMAIL PHONE' AS MATCH_SOURCE
		FROM email_phone_match_acp_id_final
	UNION ALL

	SELECT DISTINCT booking_id, schedule_start_date, acp_id, 'SHOPPER_ID' AS MATCH_SOURCE
	FROM hm44_blvd_tracking_acp_id
	where booking_id not in (sel distinct booking_id from email_phone_match_acp_id_final)		--PREFERENCE FOR EMAIL/PHONE MATCHED TO ICON_ID OR CHECKOUT
				
) with data primary index(booking_id) on commit preserve rows;

COLLECT STATISTICS COLUMN (booking_id) ON matched_acp_id; 



-- Attach Source Store for Booking & Link to Sales
-- New with Department Description & Booking Made By
--DROP TABLE T3DL_ACE_CORP.hm44_blvd_tracking_link_to_sales_SameStore;
--CREATE MULTISET table T3DL_ACE_CORP.hm44_blvd_tracking_link_to_sales_SameStore AS (
--DROP TABLE hm44_blvd_tracking_link_to_sales_SameStore;
CREATE multiset volatile TABLE hm44_blvd_tracking_link_to_sales_SameStore AS (
		SEL orig.*,
			max(case when cns.acp_id is not null then 'Y' else 'N' end) as consent_within_10,
			max(case when cns.acp_id is not null and seller_history = 'NEW TO SELLER' then 'Y' else 'N' end) as consent_within_10_new2seller,
			max(case when cns.acp_id is not null and clienteling_history = 'NEW TO CLIENTELING' then 'Y' else 'N' end) as consent_within_10_new2clienteling,
			
			max(case when cns.acp_id is not null 
						and opt_in_date_pst between schedule_start_date - 10 and schedule_start_date - 1 then 'Y' else 'N' end) as consent_within_10_prior,
			
			max(case when cns.acp_id is not null 
						and opt_in_date_pst = schedule_start_date then 'Y' else 'N' end) as consent_on_appt_dt,
						
			max(case when cns.acp_id is not null 
						and opt_in_date_pst between schedule_start_date +1 and schedule_start_date +10 then 'Y' else 'N' end) as consent_within_10_post
			
		from (
				SELECT DISTINCT 
				               a.booking_id
				              ,a.acp_id
				              ,a.schedule_start_date
				              ,b.source_store
				              ,b.booking_service_description
				              ,b.booking_made_by
				              ,store.store_dma_desc
				              ,CASE WHEN UPPER(store_dim.subgroup_desc) like 'MIDWEST%' THEN 'MW' 
				                    WHEN UPPER(store_dim.subgroup_desc) like 'NORTHEAST%' THEN 'NE'
				                    WHEN UPPER(store_dim.subgroup_desc) like 'NORTHWEST%' THEN 'NW' 
				                    WHEN UPPER(store_dim.subgroup_desc) like 'SCAL%' OR store_dim.subgroup_desc like 'SOUTHERN%' THEN 'SCAL'
				                    WHEN UPPER(store_dim.subgroup_desc) like 'SOUTHEAST%' THEN 'SE'  
				                    WHEN UPPER(store_dim.subgroup_desc) like 'SOUTHWEST%' THEN 'SW'
				                    WHEN UPPER(store_dim.subgroup_desc) like '%CANADA%' THEN 'CA'
				                    WHEN UPPER(store_dim.subgroup_desc) IS NULL THEN ''
				                    ELSE 'MISC' END AS region_group
				              ,coalesce(dtl.order_date,dtl.tran_date) purch_date
				              ,dtl.intent_store_num 
				              ,dtl.payroll_dept
				              ,d.organization_description dept_name
				              ,dtl.commission_slsprsn_num
				              --,dtl.tran_type_code 
				              --,dtl.global_tran_id 
				              --,dtl.line_item_seq_num
				              ,SUM(dtl.shipped_usd_sales) as sales_amt
				              ,SUM(dtl.shipped_qty) as units
				              ,SUM(dtl.return_usd_amt) as return_amt
				              ,SUM(dtl.return_qty) as return_qty
				              --,SUM(dtl.line_item_promo_usd_amt) as promo_amt
				--sel count(distinct b.booking_id||coalesce(b.booking_service_description,'na')||coalesce(dtl.global_tran_id,'na')||coalesce(dtl.line_item_seq_num,'na')), count(*)
				--sel count(distinct b.booking_id||coalesce(b.booking_service_description,'na')), count(*)		--very minor dup: 1,265,949	1,266,698
				FROM matched_acp_id a
				LEFT JOIN (sel distinct booking_id, schedule_start_date, source_store, booking_service_description, booking_made_by	--granular on booking_id||booking_service_id; roll-up to service_desc
							FROM hm44_blvd_tracking_discovery_booking_refined		--UPDATE FOR MORE relevant filtered down booking_service_desc (prioritizing rescheduled over scheduled)
							--from PRD_NAP_USR_VWS.BOOKING_OBJECT_MODEL_FACT
								) b ON a.booking_id = b.booking_id 
									and a.schedule_start_date = b.schedule_start_date		--PULL BY DATE/STORE of non-cancelled appt allows us to match up to sales
				--LEFT JOIN prd_nap_base_vws.jwn_store_dim_vw store ON trim(b.source_store) = trim(store.store_num) -- changed on Apr. 17th
				LEFT JOIN prd_nap_usr_vws.jwn_store_dim_vw store ON trim(b.source_store) = trim(store.store_num) -- changed on Apr. 17th
				LEFT JOIN prd_nap_usr_vws.store_dim store_dim  ON trim(b.source_store) = trim(store_dim.store_num) -- changed on Apr. 17th ), 
				
				LEFT JOIN T2DL_DAS_SALES_RETURNS.sales_and_returns_fact dtl ON a.acp_id = dtl.acp_id 
																			and trim(b.source_store) = trim(dtl.intent_store_num) -- Use Intent Store Number (trim added on Apr. 17th)
																			and a.schedule_start_date = coalesce(dtl.order_date,dtl.tran_date) -- Use either transaction date or order date!
				LEFT JOIN (SELECT * FROM PRD_NAP_HR_USR_VWS.HR_WORKER_ORG_DIM WHERE organization_type = 'Department' and organization_description not like'%inactive%') d 
																			ON dtl.payroll_dept = d.organization_code
				WHERE source_store <> ''
				
				GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
		) orig
		LEFT JOIN T2DL_DAS_CLIENTELING.consented_customer_relationship_hist cns on orig.acp_id = cns.acp_id
											and orig.schedule_start_date between cns.opt_in_date_pst -10 and cns.opt_in_date_pst +10
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
		
--) with data primary index(booking_id);
) with data primary index(booking_id) on commit preserve rows;




--REFRESHING ENTIRE TABLE TO GET ALL RETURNS
DELETE FROM {cli_t2_schema}.appointments_driven_value;


INSERT INTO {cli_t2_schema}.appointments_driven_value
    	SELECT DISTINCT 
			booking_id, 
            acp_id, 
            schedule_start_date, 
            source_store, 
            booking_service_description, 
            booking_made_by, 
            store_dma_desc, 
            region_group, 
            purch_date, 
            intent_store_num, 
            payroll_dept, 
            dept_name, 
            commission_slsprsn_num, 
            sales_amt, 
            units, 
            return_amt, 
            return_qty, 
            consent_within_10, 
            consent_within_10_new2seller, 
            consent_within_10_new2clienteling, 
            consent_within_10_prior, 
            consent_on_appt_dt, 
            consent_within_10_post,
	        CURRENT_TIMESTAMP AS dw_sys_load_tmstp
		FROM hm44_blvd_tracking_link_to_sales_SameStore
		where schedule_start_date is not NULL
			and schedule_start_date between DATE '2022-08-01' AND DATE '2025-12-31'
;




COLLECT STATISTICS COLUMN (booking_id) ON {cli_t2_schema}.appointments_driven_value;
COLLECT STATISTICS COLUMN (acp_id) ON {cli_t2_schema}.appointments_driven_value;

grant select on {cli_t2_schema}.appointments_driven_value to public;


SET QUERY_BAND = NONE FOR SESSION;
