SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=ddl_consented_customer_relationship_hist_11521_ACE_ENG;
     Task_Name=ddl_consented_customer_relationship_hist;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: June 3, 2024
*/

--drop table dat;
create multiset volatile table dat as (	
	sel distinct
		a.consented_action_request_date_pst, 		
		a.consented_action_request_timestamp_pst, 		--select UpdatedTime + '23:59:59'
		a.customer_id, a.customer_id_type,
		a.seller_id, 
		a.consented_action_request_type,
		'NEW UAT' as tableType
	from PRD_NAP_USR_VWS.CONSENTED_CLIENTELING_RELATIONSHIP_FACT a

) with data primary index(customer_id) on commit preserve rows;


/***************************************
 * 
 * 		START PULLING MARKETABILITY INFO
 * 
 ***************************************/

--drop table ocp1;
create multiset volatile table ocp1 as (
	select distinct 
			a.icon_id as provided_icon_id
			,c.acp_id
	        ,c.icon_id
	        ,COALESCE(ocp.ocp_id, c.icon_id) AS ocp_id
	        ,COALESCE(ocp.event_timestamp, date '1901-01-01') AS event_timestamp
	,rank() over (partition by c.acp_id order by event_timestamp desc) as rank_num
	
	from (select distinct customer_id as icon_id from dat) a 
	--link old icon_id to new acp_id
	left join (select acp_id, cust_id as icon_id 
	            from prd_nap_usr_vws.acp_analytical_cust_xref 		
	            where cust_source = 'icon')b on a.icon_id = b.icon_id	
	--link acp_id to current icon_id
	left join (select acp_id, cust_id as icon_id 
	            from prd_nap_usr_vws.acp_analytical_cust_xref 
	            where cust_source = 'icon')c on b.acp_id = c.acp_id
	--link current icon_id to ocp_id
	left join (select unique_source_id as customer_id, program_index_id as ocp_id, object_event_tmstp as event_timestamp 
	            from prd_nap_usr_vws.customer_obj_program_dim 		
	            where customer_source = 'icon' 
	            and program_index_name = 'OCP')ocp on 'icon::'||c.icon_id = ocp.customer_id		--icon_id --> ocp_id		1,112,769
	            
	--left join emp_ind emp on a.acp_id = emp.acp_id
	--where emp.acp_id is null 
	    qualify rank_num = 1						
)with data PRIMARY INDEX(ocp_id) on commit preserve rows;

   
COLLECT STATISTICS COLUMN (ocp_id) ON ocp1; 


--drop table current_loyalty;
create volatile table current_loyalty as (
    select a.provided_icon_id, a.acp_id, a.ocp_id, copd.loyalty_id, lty.rewards_level
    from ocp1 a
    left join (select unique_source_id, program_index_id as loyalty_id
            from prd_nap_usr_vws.customer_obj_program_dim
            where program_index_name = 'MTLYLTY') copd
    on 'icon::'||a.ocp_id = copd.unique_source_id
    left join (select loyalty_id, rewards_level 
                from prd_nap_usr_vws.loyalty_member_dim_vw 
                where current_date between rewards_level_start_date and rewards_level_end_date)lty
        on copd.loyalty_id = lty.loyalty_id
)with data on commit preserve rows;
   
   
   
CREATE MULTISET VOLATILE TABLE distinct_xref AS(
    SELECT distinct unique_source_id, tokenized_mobile
		, tokenized_email_id			--<update> FOR NEW V3 MARKETABILITY
	FROM prd_nap_usr_vws.customer_wholesession_dim
) WITH DATA PRIMARY INDEX(unique_source_id)ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (unique_source_id) ON distinct_xref; 


--drop table gc_list;
CREATE MULTISET VOLATILE TABLE gc_list AS(
	SELECT *
	FROM (SELECT * FROM current_loyalty) t1
	LEFT JOIN (SELECT * FROM distinct_xref) t2
	ON 'icon::'||t1.ocp_id = t2.unique_source_id
) WITH DATA PRIMARY INDEX(tokenized_email_id, tokenized_mobile) ON COMMIT PRESERVE ROWS;
   
COLLECT STATISTICS COLUMN (tokenized_email_id, tokenized_mobile) ON gc_list; 


	--drop table tokenized_phone;
CREATE MULTISET VOLATILE TABLE tokenized_phone AS (		--UPDATE FOR NEW V3 MARKETABILITY
	SELECT t1.tokenized_value, t2.preference_value
	FROM (
			(select * from prd_nap_usr_vws.cpp_preference_master_dim where preference_type = 'TELEPHONE') t1
	        inner join
	        (select * from prd_nap_usr_vws.cpp_preference_contact_dim where preference_name = 'MANUAL') t2
	        						ON t1.enterprise_id = t2.enterprise_id)
	WHERE tokenized_value IN (SELECT tokenized_mobile FROM gc_list)

) WITH DATA PRIMARY INDEX(tokenized_value) ON COMMIT PRESERVE ROWS;


--drop table tokenized_email;
CREATE MULTISET VOLATILE TABLE tokenized_email AS (

	SELECT t1.tokenized_value, t2.preference_value
	  FROM (
			(select * from prd_nap_usr_vws.cpp_preference_master_dim where preference_type = 'EMAIL') t1
			INNER JOIN 
			(SELECT * FROM prd_nap_usr_vws.CPP_PREFERENCE_CONTACT_DIM where preference_name = 'CONTACT'	/*and preference_value = 'Y'*/) t2	--email pref must be YES
							ON t1.enterprise_id = t2.enterprise_id
			)	   
  	WHERE tokenized_value IN (SELECT tokenized_email_id FROM gc_list)
  		
) WITH DATA PRIMARY INDEX(tokenized_value) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tokenized_value) ON tokenized_phone; 
COLLECT STATISTICS COLUMN (tokenized_value) ON tokenized_email; 


--drop table market_table;
CREATE VOLATILE MULTISET TABLE market_table AS (
    SELECT gc.*,
    		max(em.preference_value) AS em_preference_value,
    		max(ph.preference_value) AS ph_preference_value
    		
    FROM gc_list gc
    left JOIN tokenized_email em ON gc.tokenized_email_id = em.tokenized_value
    LEFT JOIN tokenized_phone ph ON gc.tokenized_mobile = ph.tokenized_value
    group by 1,2,3,4,5,6,7,8
    
) with data primary index(provided_icon_id) on commit preserve rows;



--drop table pre_attributes;
create multiset volatile table pre_attributes as (
 	select distinct 
    		a.provided_icon_id, a.acp_id, a.rewards_level, 
    		a.em_preference_value,
			a.ph_preference_value,
			
			rank() over (partition by a.provided_icon_id 
						order by case when a.ph_preference_value = 'Y' then 3 
							when a.ph_preference_value = 'N' then 2 
							when a.ph_preference_value = 'NA' then 1 else null end desc,
							a.em_preference_value desc, 
							a.rewards_level desc,
							a.ocp_id desc) as rank_num		--AM add to fix duplication

	from market_table a  
	qualify rank_num = 1
)with data primary index(provided_icon_id, acp_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (provided_icon_id, acp_id) ON pre_attributes; 

/***************************************
 * 
 * 		END PULLING MARKETABILITY INFO
 * 
 ***************************************/


--STEP 1: CONVERT TO ACP_ID
--drop table consented_acp;
create multiset volatile table consented_acp as (

	sel a.consented_action_request_date_pst, 
		a.consented_action_request_timestamp_pst, 
		a.customer_id, 
		a.customer_id_type,
		a.seller_id, 
		a.consented_action_request_type,
		max(b.acp_id) as acp_id
	--from PRD_NAP_BASE_VWS.CONSENTED_CLIENTELING_RELATIONSHIP_FACT a
	from dat a
	join pre_attributes b on a.customer_id = b.provided_icon_id
	
	group by 1,2,3,4,5,6
) with data primary index(acp_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (acp_id) ON consented_acp; 


--STEP 2: PROCESSING FILTERING OUT CASES WHERE FIRST ROW IS OPT-OUT
--drop table consented_acp_clean;
create multiset volatile table consented_acp_clean as (

	sel 
		a.consented_action_request_date_pst,
		a.consented_action_request_timestamp_pst,
		a.consented_action_request_type,
		a.acp_id,
		a.seller_id
	from 
		(sel * from consented_acp
		minus
		sel b.* from (
			sel consented_action_request_date_pst,
				consented_action_request_timestamp_pst,
				
				customer_id, customer_id_type,
				seller_id, 
				consented_action_request_type,
				acp_id

			from consented_acp
			--where consented_action_request_type = 'OPT_OUT'
			qualify row_number() over (partition by acp_id, seller_id order by consented_action_request_timestamp_pst) = 1
			) b
		where consented_action_request_type = 'OPT_OUT'
		) a
		
) with data primary index(acp_id) on commit preserve rows;

		
COLLECT STATISTICS COLUMN (acp_id) ON consented_acp_clean; 


--drop table consented_acp_clean2;
create multiset volatile table consented_acp_clean2 as (
	--CHECKING FOR CONSECUTIVE OPT-INs OR OPT-OUTs
	--STEP 3. ADDS NUMBER FOR ACP_SELLER ACTION
	--sel consented_action_request_type, next_action_type, count(distinct acp_id||seller_id) from (		--VALIDN
			sel distinct
				acp_id,
				seller_id,
				consented_action_request_date_pst,
				consented_action_request_type,
				next_action_date_pst,
				next_action_type,
				row_number() over (partition by acp_id, seller_id order by consented_action_request_date_pst) as acp_seller_action_row_num
				
			from (
					--STEP 2. CONSOLIDATES CONSECUTIVE OPT_INS 
					--			& FILTERS OUT OPT-OUT ROWS SINCE ALREADY INCLUDED IN THEIR OWN COLUMN
					sel distinct 
						acp_id,
						seller_id,
						
						consented_action_request_date_pst,
						consented_action_request_timestamp_pst,
						consented_action_request_type,
						
						LEAD(consented_action_request_date_pst, 1, date '9999-12-31') 
								OVER (partition by acp_id, seller_id ORDER BY consented_action_request_timestamp_pst) AS next_action_date_pst,
						
						LEAD(consented_action_request_timestamp_pst, 1, date '9999-12-31') 
								OVER (partition by acp_id, seller_id ORDER BY consented_action_request_timestamp_pst) AS next_action_timestamp_pst,
								
						LEAD(consented_action_request_type, 1, 'tbd') 
								OVER (partition by acp_id, seller_id ORDER BY consented_action_request_timestamp_pst) AS next_action_type
								
					--VIEWING DATA IN ORDER
					--sel *			--VALIDN
					from (
						--STEP 1. PIVOTS CONSECUTIVE ACTIONS AS OPT-IN AND OPT-OUT SEPARATED COLUMNS
						sel acp_id,
							seller_id,
							consented_action_request_date_pst,
							consented_action_request_timestamp_pst,
							consented_action_request_type,
							
							LAG(consented_action_request_type, 1, 'unkn') 
									OVER (partition by acp_id, seller_id ORDER BY consented_action_request_timestamp_pst) AS prev_action_type,
									
							case when consented_action_request_type = prev_action_type then 1 else 0 end as mltpl_optInOut
									
						from consented_acp_clean		--has both opt in/out
					) a
					--where acp_id IN (SEL ACP_ID FROM (sel acp_id, seller_id, consented_action_request_type from consented_acp_clean having count(*)>=4 group by 1,2,3)a )--VALIDN
					--order by acp_id, seller_id, consented_action_request_timestamp_pst			--VALIDN
					where 1=1
						and a.mltpl_optInOut = 0
			) b
			where 1=1 --) test group by 1,2 order by 1,2 
				and b.consented_action_request_type = 'OPT_IN'
				and b.next_action_type in ('OPT_OUT','tbd')
				--and b.acp_id is not null 
				
) with data primary index(acp_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (acp_id) ON consented_acp_clean2; 


--drop table consented_numbered;
create multiset volatile table consented_numbered as (	
	sel a.*
	from (
			sel  
				acp_id,
				seller_id,
				consented_action_request_type,
				consented_action_request_date_pst,			
				next_action_type,
				next_action_date_pst,
				acp_seller_action_row_num,		--how long is the history with this seller_id
				min(consented_action_request_date_pst) over (partition by acp_id, seller_id) as first_seller_consent_dt,
				min(consented_action_request_date_pst) over (partition by acp_id) as first_clienteling_consent_dt,
				row_number() over (partition by acp_id
						order by consented_action_request_date_pst, seller_id, next_action_date_pst) as row_num						--how long is the history with clienteling
						
			from consented_acp_clean2
	) a

) with data primary index(acp_id) on commit preserve rows;
		

COLLECT STATISTICS COLUMN (acp_id) ON consented_numbered; 


--STEP 3: PROCESSING COMPLETE. APPENDING NEW TO ATTRIBUTES---
--drop table consented_history;
create multiset volatile table consented_history as (	
	sel a.*,
			
		max(b.rewards_level) as rewards_level,
		max(b.em_preference_value) as em_preference_value,
		max(b.ph_preference_value) as ph_preference_value
	from 
	(
		sel acp_id,
			seller_id,
			first_clienteling_consent_dt,
			first_seller_consent_dt,
			dense_rank() over (partition by acp_id order by first_seller_consent_dt, seller_id) as seller_num,
			consented_action_request_date_pst AS opt_in_date_pst,			
			next_action_date_pst AS opt_out_date_pst,
			next_action_type,
			acp_seller_action_row_num,
			row_num,
			case when row_num = 1 then 'NEW TO CLIENTELING'
				else 'EXISTING RELN W CLIENTELING'
				end as clienteling_history,
			
			case when acp_seller_action_row_num = 1 then 'NEW TO SELLER'
				else 'EXISTING RELN W SELLER'
				end as seller_history
				
		from consented_numbered
	) a
	left join pre_attributes b on a.acp_id = b.acp_id

	group by 1,2,3,4,5,6,7,8,9,10,11,12
	
) with data primary index(acp_id) on commit preserve rows;

COLLECT STATISTICS COLUMN (acp_id) ON consented_history; 


/*------- END OF STEP 01: CREATING NEW CONNECTIONS DATA SOURCE DEDUPLICATED AND ONE ROW PER CUSTOMER/SELLER------
 * 					SAVE TO OUTPUT TABLE!
 * */

DELETE FROM {cli_t2_schema}.consented_customer_relationship_hist;
    	
INSERT INTO {cli_t2_schema}.consented_customer_relationship_hist
    	SEL acp_id,
    		seller_id,
    		first_clienteling_consent_dt,
    		first_seller_consent_dt,
    		seller_num,
    		opt_in_date_pst,
    		opt_out_date_pst,
    		clienteling_history,
    		seller_history,
    		rewards_level,
    		em_preference_value AS email_pref_cur,
    		ph_preference_value AS phone_pref_cur,
			CURRENT_TIMESTAMP as dw_sys_load_tmstp
    	FROM consented_history
;

    
COLLECT STATISTICS COLUMN (ACP_ID) ON {cli_t2_schema}.consented_customer_relationship_hist; 


 grant select on {cli_t2_schema}.consented_customer_relationship_hist to public;


SET QUERY_BAND = NONE FOR SESSION;
    