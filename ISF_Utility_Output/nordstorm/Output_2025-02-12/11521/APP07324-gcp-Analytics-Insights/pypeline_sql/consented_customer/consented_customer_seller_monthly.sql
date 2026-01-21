/*

CONSENTED_CUSTOMER_SELLER_MONTHLY.SQL

*/
SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=consented_customer_seller_monthly_11521_ACE_ENG;
     Task_Name=consented_customer_seller_monthly;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: June 5, 2024
*/
    

/********************************************
 * 
 * 		PREP DATES AND HR DATA SOURCES
 * 
 * 
 ********************************************/

--drop table dates;
CREATE  MULTISET VOLATILE TABLE dates AS (
		/*----FOR TESTING
		sel 
			date '2024-02-24' as min_date, 
			current_date-1 as max_date
		
		*/	
		--for Full Reporting Months
		sel
			min(min_day_date) AS min_date, 
			max(max_day_date) AS max_date
		from	
		 ( SELECT distinct 
	        	   MAX(day_date) over (partition by fiscal_month, fiscal_quarter, fiscal_year) as max_day_date,
	        	   MIN(day_date) over (partition by fiscal_month, fiscal_quarter, fiscal_year) as min_day_date,
	        	   '('||min_day_date||' - '||max_day_date||')' as fm_range,
	        	   
	        	   --week_of_fyr AS fiscal_week,
	               month_454_num AS fiscal_month,
	               quarter_454_num AS fiscal_quarter,
	               year_num AS fiscal_year
				   --last_year_day_date_realigned AS ly_day_date
				   
	        FROM prd_nap_usr_vws.DAY_CAL
	        where day_date between date '2022-12-31' and current_date()-1+60
	        ) cal  
	      WHERE 1=1
	      		and cal.max_day_date <= current_date-1
	      		AND cal.fiscal_year >= 2023
	    --NEW LOGIC TO FILTER OUT fm_range EXISTANT IN TABLE CURRENTLY
	    		AND cal.fm_range not in (select distinct fm_range 
	    								from {cli_t2_schema}.consented_customer_seller_monthly
										--REMOVING LAST MONTH'S UPDATE FOR TESTING OF MOST RECENT MONTH LOAD
										--where fm_range <> '(2024-05-05 - 2024-06-01)'
	    								)
	     	
) with data ON COMMIT PRESERVE ROWS;




--drop table cal_dates;
CREATE  MULTISET VOLATILE TABLE cal_dates AS (
	sel a.* 
	from (
				        SELECT day_date,
				        	   MAX(day_date) over (partition by fiscal_month, fiscal_quarter, fiscal_year) as max_day_date,
				        	   MIN(day_date) over (partition by fiscal_month, fiscal_quarter, fiscal_year) as min_day_date,
								'('||min_day_date||' - '||max_day_date||')' as fm_range,
				               week_of_fyr AS fiscal_week,
				               month_454_num AS fiscal_month,
				               quarter_454_num AS fiscal_quarter,
				               year_num AS fiscal_year
							   --last_year_day_date_realigned AS ly_day_date			--THIS IS BRINGING 2/14/24
							   
				        FROM prd_nap_usr_vws.DAY_CAL dc
				        join dates d on dc.day_date between d.min_date-30 and d.max_date+30
				  ) a 
	join dates b on a.day_date between b.min_date and b.max_date
				        
) WITH DATA PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (day_date) ON cal_dates;



--create lookup table of all Non-Eligible Sales in Restaurant depts
--drop table restaurant_dept_lookup;
CREATE multiset volatile TABLE restaurant_dept_lookup AS (
	select distinct dept_num
	from prd_nap_usr_vws.DEPARTMENT_DIM  
	where division_num=70
) with data primary index(dept_num) on commit preserve rows;


--drop table hr_range;
CREATE  MULTISET VOLATILE TABLE hr_range AS (

	
			SELECT  DISTINCT 
					TD_SYSFNLIB.lpad(a.worker_number, 15,'0') AS worker_number,
	                CONCAT(nme.last_name, ', ', nme.first_name) AS employee_name,
	                --nme.worker_status,
	                nme.termination_date,
	                nme.hire_date as latest_hire_date,
	                trim(leading '0' from a.payroll_store) as payroll_store,
	                stor.store_name AS payroll_store_description,
	                dpt.organization_description AS payroll_department_description,
	                cast(coalesce(a.eff_begin_date, nme.hire_date) as date) AS eff_begin_date,	
	                cast(a.eff_end_date - INTERVAL '0:01' MINUTE TO SECOND as date) AS eff_end_date,
	                
	                min(coalesce(a.eff_begin_date, nme.hire_date)) over (partition by a.worker_number) as original_hire_date

	        FROM prd_nap_hr_usr_vws.HR_ORG_DETAILS_DIM_EFF_DATE_VW a	
					        		
	        LEFT JOIN (select 	a.worker_number, 
	        					a.last_name, a.first_name,
	        					a.hire_date,
	        					a.worker_status, a.termination_date
						from prd_nap_hr_usr_vws.hr_worker_v1_dim a	
						/* COMMENTING OUT CODE TO ONLY PULL THE LATEST DATA 
						join(select worker_number, 
								max(last_updated) as max_date 
								from prd_nap_hr_usr_vws.hr_worker_v1_dim 
								group by 1
								)b on a.worker_number = b.worker_number 
						  		 		and a.last_updated = b.max_date
						  */
						) nme ON a.worker_number = nme.worker_number
								and a.eff_begin_date between nme.hire_date and coalesce(nme.termination_date, date '9999-12-31')		--new add for historical hr worker_status
	                        			
			--for day granularity changed back to inner join (2.23.24)
	        JOIN (SELECT store_num, store_name, store_type_code FROM prd_nap_usr_vws.STORE_DIM		--CHANGED FROM INNER JOIN TO LEFT JOIN ( AM 9.28.22) 
	        			WHERE store_type_code IN ('FL')
	        			) stor ON cast(trim(LEADING '0' FROM a.payroll_store) AS varchar(10)) = cast(stor.store_num AS varchar(10))
	
	        JOIN (select organization_code, organization_description from prd_nap_hr_usr_vws.HR_WORKER_ORG_DIM									--CHANGED FROM INNER JOIN TO LEFT JOIN ( AM 9.28.22)
						where organization_type = 'DEPARTMENT'
	                      and cast(is_inactive as int)= 0
	                      )dpt  on a.payroll_department = dpt.organization_code            --DBA RECOMMENDED
						--and is_inactive = 0)dpt  on a.payroll_department = dpt.organization_code
	        WHERE a.eff_end_date >= (sel min_date from dates)       		
		
) WITH DATA PRIMARY INDEX(worker_number)--PRIMARY INDEX (worker_number)   --DBA Rec)
PARTITION BY RANGE_N(eff_begin_date between date '1970-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (worker_number) ON hr_range;
COLLECT STATISTICS COLUMN (eff_begin_date) ON hr_range;
COLLECT STATISTICS COLUMN (eff_end_date) ON hr_range;


/********************************************************
 * 
 * 		PREPPING TRANSACTIONS/TRIPS DATA
 * 
 * 
 ********************************************************/


--STEP 1: ADD SALES DURING CONSENT TO CUSTOMER-LEVEL DATA (TAGGING COMMISSION TO CONSENTED STYLIST)
--INPUT @GLOBAL_TRAN_ID||LINE_ITEM_SEQ_NUM LEVEL, @ACP_ID||SELLER_ID||OPT_IN/OUT_DATE_PST LEVEL
--OUTPUT @ sel count(distinct GLOBAL_TRAN_ID||LINE_ITEM_SEQ_NUM), count(*) from tyly_sales; LEVEL
--drop table tyly_sales;
CREATE  MULTISET VOLATILE TABLE tyly_sales AS (
		with dat as (
			sel dtl.acp_id,
		        dtl.global_tran_id,				
		        dtl.line_item_seq_num,
		        coalesce(dtl.order_date,dtl.tran_date) as sale_date,	
		        
		        dtl.line_item_quantity as item_cnt,
	
		        dtl.line_net_usd_amt as gross_amt,
		        
		        case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
		              and dtl.line_item_net_amt_currency_code = 'CAD' then 867
		            when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
		              and dtl.line_item_net_amt_currency_code = 'USD' then 808									
		            else dtl.intent_store_num end as store_num,
		           
		        case when dtl.commission_slsprsn_num is not null 
	        		and dtl.commission_slsprsn_num not in ('2079333', '2079334', '0000') 
	        		then dtl.commission_slsprsn_num else null end as commission_slsprsn_num,
	        		
	        	dtl.item_source,
	        	dtl.trip_id
		    
		    from prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
		    left join restaurant_dept_lookup r on coalesce(dtl.merch_dept_num,-1*dtl.intent_store_num)=r.dept_num
		    where trim(coalesce(dtl.upc_num,'-1'))<>'-1'  --Filters out Non-Merch or Restaurant (RPOS)
				and dtl.acp_id is not null
				and dtl.line_net_usd_amt > 0 
				and coalesce(dtl.nonmerch_fee_code,'na') <> '6666'	--FILTERING OUT PURCHASES MADE TOWARDS BUYING GIFT CARD
				and dtl.business_day_date > date '2022-01-01'  
				and sale_date > date '2022-01-01'
				--(sel min(opt_in_date_pst) as min_date from T2DL_DAS_CLIENTELING.consented_customer_relationship_hist)
				and sale_date between (sel min_date from dates) and (sel max_date from dates)
				and r.dept_num is null 
				
		)
		
		sel distinct
			dat.*,
			str.business_unit_desc,
	       
	        case when dat.commission_slsprsn_num is not null then 'Y' else 'N' end as sale_w_commissioned_seller,	        		
	        max(case when dat.commission_slsprsn_num = c.seller_id then 'Y' else 'N' end) over (partition by global_tran_id||line_item_seq_num) as sale_w_consented_seller,	        					
			case when dat.item_source = 'SB_SALESEMPINIT' then 'Y' else 'N' end as digital_styling

	    from dat
		join prd_nap_usr_vws.store_dim as str  on dat.store_num = str.store_num
											and str.business_unit_desc in ('FULL LINE','N.COM')			--dropped to 83,620,623
											
	    --PREPARE TO MAKE THIS LEFT JOIN FOR ALL CUSTOMERS
		join T2DL_DAS_CLIENTELING.consented_customer_relationship_hist c on dat.acp_id = c.acp_id
									and dat.sale_date between c.opt_in_date_pst and c.opt_out_date_pst
		
) with data primary index(acp_id, global_tran_id) 
PARTITION BY RANGE_N(sale_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (ACP_ID) ON tyly_sales;
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON tyly_sales;
COLLECT STATISTICS COLUMN (SALE_DATE) ON tyly_sales;
COLLECT STATISTICS COLUMN (commission_slsprsn_num) ON tyly_sales;


--SEL * FROM tyly_sales;
--SEL count(distinct trip_id) as newt, count(distinct trip_id_old) as oldt, (newt*1.0000/oldt)-1 as pct_diff FROM tyly_sales;
--Checked zero change in move to trip_id attribute for counting trips


--STEP 2b: APPEND HR 
--COMMISSION_SLSPRSN_NUM takes on values (<CONSENTED SELLER ID>, 'NON-CONSENTED SELLER', NULL)
--RECONSIDER ADDING TERMINATED/STORE CLOSED ALL AS ATTRIBUTES TO METHOD OF COMMISSION (ADDL ROWS)
--drop table tyly_sales_HR;
CREATE  MULTISET VOLATILE TABLE tyly_sales_HR AS (
		sel ccc.*,
			
		   max(cal.fm_range) over (partition by sale_date) as fm_range,
	       max(cal.fiscal_month) over (partition by sale_date) as fiscal_month,
	       max(cal.fiscal_quarter) over (partition by sale_date) as fiscal_quarter,
	       max(cal.fiscal_year) over (partition by sale_date) as fiscal_year,
		   
		   --SELLER attributes (worker_status, seller_id, seller_name, payroll_store, payroll_store_description, payroll_department_description)
	       CASE WHEN (	
	       			(ccc.sale_date>= coalesce(hr.termination_date, date '9999-12-31') 
		   			and ccc.sale_date <= coalesce(hr.latest_hire_date, date '9999-12-31'))
				or
					 (ccc.sale_date >= coalesce(hr.termination_date, date '9999-12-31') 
					 and hr.termination_date> hr.latest_hire_date)
				)THEN 'TERMINATED'
				WHEN ccc.commission_slsprsn_num is null then null 
				ELSE 'ACTIVE' END AS worker_status,
						--use case 1) seller terminated (NULL latest_hire_date) --> post termination date, shows as TERMINATED
						--use case 2) seller terminated and later re-hired --> TERMINATED when in between termination and latest_hire_dates
						--use case 3) seller never terminated (NULL termination_date) ---> as long as seller in HR tables, always ACTIVE*/
			
		    CASE WHEN WORKER_STATUS = 'TERMINATED' THEN 'TERMINATED' ELSE 	ccc.commission_slsprsn_num END AS seller_id,
			CASE WHEN WORKER_STATUS = 'TERMINATED' THEN 'TERMINATED' ELSE 	hr.employee_name END AS seller_name,
			
	        case when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_store end as payroll_store,
	        CASE when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_store_description end AS payroll_store_description,
	        CASE when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_department_description END AS payroll_department_description			--JUST ADDED IN WITHOUT CAUSING DUPLICATION, CONSIDER ADDING INTO VIZ
	        	        
			from tyly_sales ccc
			join cal_dates cal on ccc.sale_date = cal.day_date
			left join hr_range hr on lpad(ccc.commission_slsprsn_num,15,'0') = hr.worker_number
								and ccc.sale_date between hr.eff_begin_date and hr.eff_end_date

) with data primary index(acp_id, global_tran_id) 
PARTITION BY RANGE_N(sale_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;

--sel * from tyly_sales_HR;

COLLECT STATISTICS COLUMN (ACP_ID) ON tyly_sales_hr;
COLLECT STATISTICS COLUMN (global_tran_id) ON tyly_sales_hr;
COLLECT STATISTICS COLUMN (sale_date) ON tyly_sales_hr;
COLLECT STATISTICS COLUMN (commission_slsprsn_num) ON tyly_sales_hr;



--Checked output is 1:1 with trip_id
--sel count(distinct trip_id), count(*) from tyly_sales_daily_tripShare;


--STEP 2a: ROLL UP TO DAY ("trip") GRANULARITY, FILTER ON FLS/NCOM AND SEGMENT SALES BY FLS/NCOM  --FOR TRIP ROLL UP TO SELLER WITH HIGHEST SALES IN TRIP, NOTE SALES ATTRIBUTION ARE BEIGN ROLLED UP TO TRIP LEVEL AS WELL!
--@ DAY_DATE||ACP_ID||COMMISSION_SLSPRSN_NUM||DIGITAL_STYLING LEVEL
--COMMISSION_SLSPRSN_NUM takes on values (<CONSENTED SELLER ID>, 'NON-CONSENTED SELLER', NULL)
--drop table tyly_sales_daily_tripShare;
CREATE  MULTISET VOLATILE TABLE tyly_sales_daily_tripShare AS (
		sel distinct
			a.sale_date, 
			a.fm_range,	
	        a.fiscal_month, a.fiscal_quarter,a.fiscal_year,
			a.trip_id,
			
			--Can consider more Customer Attributes
			a.acp_id,
			
			--Seller Attributes
			a.sale_w_commissioned_seller as TS_sale_w_commissioned_seller,	--Y/N COMMISSIONED TO ANY SELLER
			a.sale_w_consented_seller as TS_sale_w_consented_seller,	--Y/N COMMISSION TO CONSENTED SELLER
			
			case when a.sale_w_consented_seller = 'Y' then a.commission_slsprsn_num 
					when a.commission_slsprsn_num is not null then 'Non-Consented Seller'		--still commissioned but not consented (DEBATING IF WE WANT TO GROUP NON-CONSENTED OR POSSIBLY COUNT TRIP BY COUNT OF SELLERS)
					else null 
					end as TS_commission_slsprsn_num,								--COMMISSIONED CONSENTED SELLER ID
								
			case when a.sale_w_consented_seller = 'Y' then a.seller_name 
					when a.commission_slsprsn_num is not null then 'Non-Consented Seller'		--still commissioned but not consented (DEBATING IF WE WANT TO GROUP NON-CONSENTED OR POSSIBLY COUNT TRIP BY COUNT OF SELLERS)
					else null 
					end as TS_seller_name, 
									
			case when a.sale_w_consented_seller = 'Y' then a.worker_status 
					when a.commission_slsprsn_num is not null then 'Non-Consented Seller'		--still commissioned but not consented (DEBATING IF WE WANT TO GROUP NON-CONSENTED OR POSSIBLY COUNT TRIP BY COUNT OF SELLERS)
					else null 
					end as TS_worker_status, 
					
			a.payroll_store, 
			a.payroll_store_description, 
			a.payroll_department_description,
			
			case when a.business_unit_desc = 'FULL LINE' then roll.trip_gross_sales else 0 end as TS_gross_FLS_spend,
			case when a.business_unit_desc = 'FULL LINE' then 1 else 0 end as TS_FLS_trips,
			case when a.business_unit_desc = 'N.COM' then roll.trip_gross_sales else 0 end as TS_gross_NCOM_spend,
			case when a.business_unit_desc = 'N.COM' then 1 else 0 end as TS_NCOM_trips,
			case when a.business_unit_desc = 'N.COM' and a.digital_styling = 'Y' then roll.trip_gross_sales else 0 end as TS_sb_gross_NCOM_spend,
			case when a.business_unit_desc = 'N.COM' and a.digital_styling = 'Y' then 1 else 0 end as TS_sb_NCOM_trips
			
		from tyly_sales_hr a
		join (sel rnk.*,
					dense_rank() over (partition by trip_id 
											order by sale_w_consented_seller desc, 
													digital_styling desc, 
													sale_w_commissioned_seller desc, 
													trip_gross_sales desc,
													worker_status,
													seller_id,
													commission_slsprsn_num,
													payroll_department_description) as rankNbr
				from 
				(sel distinct 
						trip_id,
						coalesce(seller_id,'na') as seller_id,
						sale_w_commissioned_seller,
						sale_w_consented_seller,
						digital_styling,
						coalesce(worker_status,'na') as worker_status,
						coalesce(commission_slsprsn_num,'na') as commission_slsprsn_num,
						coalesce(payroll_department_description,'na') as payroll_department_description,
						
						sum(gross_amt) over (partition by trip_id) as trip_gross_sales
						
				from tyly_sales_hr
				) rnk
				--order by trip_id, RANKNBR
				qualify rankNbr = 1
				
			) roll on a.trip_id = roll.trip_id
					and coalesce(a.seller_id,'na') = roll.seller_id
					and a.sale_w_commissioned_seller = roll.sale_w_commissioned_seller
					and a.sale_w_consented_seller = roll.sale_w_consented_seller
					and a.digital_styling = roll.digital_styling
					and coalesce(a.worker_status,'na') = roll.worker_status
					and coalesce(a.commission_slsprsn_num,'na') = roll.commission_slsprsn_num
					and coalesce(a.payroll_department_description,'na') = roll.payroll_department_description
			
						
) with data primary index(acp_id, trip_id) 
PARTITION BY RANGE_N(sale_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (ACP_ID) ON tyly_sales_daily_tripShare;
COLLECT STATISTICS COLUMN (trip_id) ON tyly_sales_daily_tripShare;
COLLECT STATISTICS COLUMN (sale_date) ON tyly_sales_daily_tripShare;



--STEP 2a: ROLL UP TO DAY ("trip") GRANULARITY, FILTER ON FLS/NCOM AND SEGMENT SALES BY FLS/NCOM  --FOR TRIP ROLL UP TO SELLER WITH HIGHEST SALES IN TRIP, NOTE SALES ATTRIBUTION ARE BEIGN ROLLED UP TO TRIP LEVEL AS WELL!
--@ DAY_DATE||ACP_ID||COMMISSION_SLSPRSN_NUM||DIGITAL_STYLING LEVEL
--COMMISSION_SLSPRSN_NUM takes on values (<CONSENTED SELLER ID>, 'NON-CONSENTED SELLER', NULL)
--drop table tyly_sales_daily_sellerShare;
CREATE  MULTISET VOLATILE TABLE tyly_sales_daily_sellerShare AS (
		sel 
			a.sale_date, 
			a.fm_range,	
	        a.fiscal_month, a.fiscal_quarter,a.fiscal_year,
			a.trip_id,
			
			--Can consider more Customer Attributes
			a.acp_id,
			
			--Seller Attributes
			a.sale_w_commissioned_seller,	--Y/N COMMISSIONED TO ANY SELLER
			a.sale_w_consented_seller,	--Y/N COMMISSION TO CONSENTED SELLER
			commission_slsprsn_num,
			
			case when a.sale_w_consented_seller = 'Y' then a.seller_name 
					when a.commission_slsprsn_num is not null then 'Non-Consented Seller'		--still commissioned but not consented (DEBATING IF WE WANT TO GROUP NON-CONSENTED OR POSSIBLY COUNT TRIP BY COUNT OF SELLERS)
					else null end as seller_name, 
									
			case when a.sale_w_consented_seller = 'Y' then a.worker_status 
					when a.commission_slsprsn_num is not null then 'Non-Consented Seller'		--still commissioned but not consented (DEBATING IF WE WANT TO GROUP NON-CONSENTED OR POSSIBLY COUNT TRIP BY COUNT OF SELLERS)
					else null end as worker_status, 
					
			a.payroll_store, 
			a.payroll_store_description, 
			a.payroll_department_description,
			
		--double-count trip logic
			case when a.business_unit_desc = 'FULL LINE' then 1 else 0 end as FLS_trips,
			case when a.business_unit_desc = 'N.COM' then 1 else 0 end as NCOM_trips,
			case when a.business_unit_desc = 'N.COM' and a.digital_styling = 'Y' then 1 else 0 end as sb_NCOM_trips,
			sum(case when a.business_unit_desc = 'FULL LINE' then coalesce(a.gross_amt,0) else 0 end) as gross_FLS_spend,
			sum(case when a.business_unit_desc = 'N.COM' then coalesce(a.gross_amt,0) else 0 end) as gross_NCOM_spend,
			sum(case when a.business_unit_desc = 'N.COM' and a.digital_styling = 'Y' then coalesce(a.gross_amt,0) else 0 end) as sb_gross_NCOM_spend
			
		FROM tyly_sales_hr a
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
								
) with data primary index(acp_id, trip_id) 
PARTITION BY RANGE_N(sale_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (ACP_ID) ON tyly_sales_daily_sellerShare;
COLLECT STATISTICS COLUMN (trip_id) ON tyly_sales_daily_sellerShare;
COLLECT STATISTICS COLUMN (sale_date) ON tyly_sales_daily_sellerShare;




--STEP 3: ROLL DAY UP TO MONTHLY
--@ fm_range||ACP_ID||COMMISSION_SLSPRSN_NUM LEVEL
--drop table tyly_sales_weekly;
CREATE  MULTISET VOLATILE TABLE tyly_sales_weekly AS (

		sel distinct
			dbl.fm_range,
			dbl.acp_id, 
			dbl.fiscal_month, dbl.fiscal_quarter,dbl.fiscal_year,
			
			--seller attributes
			dbl.seller_id,		--values: 'Non-Consented Seller', consented seller id, or null
			dbl.seller_name, dbl.worker_status, dbl.payroll_store, dbl.payroll_store_description, dbl.payroll_department_description,
			
	--TOT acp||seller-row (source of truth for SELLER--> but double-counted trip attribution due i) to multiple sellers sharing 1 trip, ii) trip containing sb and non)
		    dbl.FLS_trips as seller_FLS_trips,
		    dbl.NCOM_trips as seller_NCOM_trips,
		    dbl.sb_NCOM_trips as seller_sb_NCOM_trips,
		    
			dbl.gross_FLS_spend as seller_gross_FLS_spend,
		    dbl.gross_NCOM_spend as seller_gross_NCOM_spend,
		    dbl.sb_gross_NCOM_spend as seller_sb_gross_NCOM_spend,
		    
	--TOT trip_id level (source of truth for TRIP reporting --> but not as accurate at the seller level due to overlap)
		    coalesce(ts.TS_FLS_trips,0) as TS_FLS_trips,
		    coalesce(ts.TS_NCOM_trips,0) as TS_NCOM_trips,
		    
			coalesce(ts.TS_gross_FLS_spend,0) as TS_gross_FLS_spend,
		    coalesce(ts.TS_gross_NCOM_spend,0) as TS_gross_NCOM_spend,
		    coalesce(ts.TS_sb_gross_NCOM_spend,0) as TS_sb_gross_NCOM_spend,
		    coalesce(ts.TS_sb_NCOM_trips,0) as TS_sb_NCOM_trips,
		    
	--TOT acp-level activity @month||cust level
			max(coalesce(ts.TS_acp_FLS_trips,0)) over (partition by dbl.fm_range, dbl.acp_id)  as TS_acp_FLS_trips,
		    max(coalesce(ts.TS_acp_NCOM_trips,0)) over (partition by dbl.fm_range, dbl.acp_id) as TS_acp_NCOM_trips,
		    
			max(coalesce(ts.TS_acp_FLS_commissioned_trips,0)) over (partition by dbl.fm_range, dbl.acp_id) as TS_acp_FLS_commissioned_trips,
		    max(coalesce(ts.TS_acp_NCOM_commissioned_trips,0)) over (partition by dbl.fm_range, dbl.acp_id) as TS_acp_NCOM_commissioned_trips,
		    
			max(coalesce(ts.TS_acp_FLS_consented_trips,0)) over (partition by dbl.fm_range, dbl.acp_id) as TS_acp_FLS_consented_trips,
		    max(coalesce(ts.TS_acp_NCOM_consented_trips,0)) over (partition by dbl.fm_range, dbl.acp_id) as TS_acp_NCOM_consented_trips,
		    
			max(coalesce(ts.TS_acp_sb_NCOM_trips,0)) over (partition by dbl.fm_range, dbl.acp_id) as TS_acp_sb_NCOM_trips
			
		from (
			sel distinct
			fm_range,
			acp_id, 
			fiscal_month, fiscal_quarter,fiscal_year,	
			
			--seller attributes
			case when sale_w_consented_seller = 'Y' then commission_slsprsn_num 
					when commission_slsprsn_num is not null then 'Non-Consented Seller'		--still commissioned but not consented (DEBATING IF WE WANT TO GROUP NON-CONSENTED OR POSSIBLY COUNT TRIP BY COUNT OF SELLERS)
					else null end as seller_id,		--values: 'Non-Consented Seller', consented seller id, or null
			seller_name, worker_status, payroll_store, payroll_store_description, payroll_department_description,
			
	--TOT acp||seller-row
			sum(gross_FLS_spend) over (partition by fm_range, acp_id, worker_status, seller_id, 
													payroll_store, payroll_department_description) as gross_FLS_spend,
		    sum(FLS_trips) over (partition by fm_range, acp_id, worker_status, seller_id, 
		    										payroll_store, payroll_department_description) as FLS_trips,
		    
		    sum(gross_NCOM_spend) over (partition by fm_range, acp_id, worker_status, seller_id, 
		    										payroll_store, payroll_department_description) as gross_NCOM_spend,
		    sum(NCOM_trips) over (partition by fm_range, acp_id, worker_status, seller_id, 
		    										payroll_store, payroll_department_description) as NCOM_trips,
		
		    sum(sb_gross_NCOM_spend) over (partition by fm_range, acp_id, worker_status, seller_id, 
		    										payroll_store, payroll_department_description) as sb_gross_NCOM_spend,
		    sum(sb_NCOM_trips) over (partition by fm_range, acp_id, worker_status, seller_id, 
		    										payroll_store, payroll_department_description) as sb_NCOM_trips
		    
			from tyly_sales_daily_sellerShare
			--where fm_range||acp_id = '(2024-02-04 - 2024-03-02)amp::00417662-3984-31ee-b408-1a8e39cc5c5f'

		) dbl
		left join 
		(
			sel distinct
			fm_range,
			acp_id, 
			fiscal_month, fiscal_quarter,fiscal_year,
			
			--seller attributes
			TS_commission_slsprsn_num,		--values: 'Non-Consented Seller', consented seller id, or null
			TS_seller_name, TS_worker_status, payroll_store, payroll_store_description, payroll_department_description,
			
	--TOT acp||seller-row
			sum(TS_gross_FLS_spend) over (partition by fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) as TS_gross_FLS_spend,
		    sum(TS_FLS_trips) over (partition by fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) as TS_FLS_trips,
		    
		    sum(TS_gross_NCOM_spend) over (partition by fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) as TS_gross_NCOM_spend,
		    sum(TS_NCOM_trips) over (partition by fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) as TS_NCOM_trips,
		
		    sum(TS_sb_gross_NCOM_spend) over (partition by fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) as TS_sb_gross_NCOM_spend,
		    sum(TS_sb_NCOM_trips) over (partition by fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) as TS_sb_NCOM_trips,
		    
		    
	--TOT acp-level activity @month||cust level
			sum(TS_FLS_trips) over (partition by fm_range, acp_id) as TS_acp_FLS_trips,
		    sum(TS_NCOM_trips) over (partition by fm_range, acp_id) as TS_acp_NCOM_trips,
		    
			sum(case when TS_sale_w_commissioned_seller = 'Y' then TS_FLS_trips end) over (partition by fm_range, acp_id) as TS_acp_FLS_commissioned_trips,
		    sum(case when TS_sale_w_commissioned_seller = 'Y' then TS_NCOM_trips end) over (partition by fm_range, acp_id) as TS_acp_NCOM_commissioned_trips,
		    
			sum(case when TS_sale_w_consented_seller = 'Y' then TS_FLS_trips end) over (partition by fm_range, acp_id) as TS_acp_FLS_consented_trips,
		    sum(case when TS_sale_w_consented_seller = 'Y' then TS_NCOM_trips end) over (partition by fm_range, acp_id) as TS_acp_NCOM_consented_trips,
		    
			sum(TS_sb_NCOM_trips) over (partition by fm_range, acp_id) as TS_acp_sb_NCOM_trips
		
			from tyly_sales_daily_tripShare
		) ts on dbl.fm_range=ts.fm_range
				and dbl.acp_id=ts.acp_id
				and coalesce(dbl.seller_id,'na')=coalesce(ts.TS_commission_slsprsn_num,'na')
				and coalesce(dbl.payroll_store,'na')=coalesce(ts.payroll_store,'na')
				and coalesce(dbl.payroll_department_description,'na')=coalesce(ts.payroll_department_description,'na')
		
) with data primary index(acp_id) on commit preserve rows;


COLLECT STATISTICS COLUMN (ACP_ID) ON tyly_sales_weekly;
COLLECT STATISTICS COLUMN (fm_range) ON tyly_sales_weekly;


COLLECT STATISTICS COLUMN (worker_number, eff_begin_date, eff_end_date) ON hr_range;

COLLECT STATISTICS COLUMN (ACP_ID, GLOBAL_TRAN_ID, SALE_DATE, commission_slsprsn_num) ON tyly_sales;

COLLECT STATISTICS COLUMN (ACP_ID, GLOBAL_TRAN_ID, SALE_DATE, commission_slsprsn_num) ON tyly_sales_hr;

COLLECT STATISTICS COLUMN (ACP_ID, trip_id, sale_date) ON tyly_sales_daily_tripShare;

COLLECT STATISTICS COLUMN (ACP_ID, trip_id, sale_date) ON tyly_sales_daily_sellerShare;

COLLECT STATISTICS COLUMN (ACP_ID, fm_range) ON tyly_sales_weekly;


/********************************************************
 * 
 * 		PREPPING CONSENTED CUSTOMER DATA
 * 
 * 
 ********************************************************/

--sel * from consented_daily where acp_id is null;

--STEP 4: ADD CUSTOMER LEVEL RELATIONSHIP DETAIL
--@ fm_range||ACP_ID||COMMISSION_SLSPRSN_NUM LEVEL
--drop table consented_daily_pt1;
CREATE  MULTISET VOLATILE TABLE consented_daily_pt1 AS (


			SELECT 
	                cc.acp_id,
					--a.day_date,
					a.fm_range,
					a.min_day_date, a.max_day_date,
	                a.fiscal_month, a.fiscal_quarter,a.fiscal_year,
	                --a.ly_day_date,
	                
	               --CUSTOMER ATTRIBUTES
	                cc.first_seller_consent_dt,
	                cc.first_clienteling_consent_dt,
	                
				   --V1 SELLER attributes (worker_status, seller_id, seller_name, payroll_store, payroll_store_description, payroll_department_description)
			        --cc.seller_id,
	                /*--validation start
	                sel distinct a.day_date, hr.termination_date, hr.latest_hire_date, case WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED' end payroll, 
	                	cc.seller_id,
	                --validation update end*/
	                --V2 SELLER attributes (worker_status, seller_id, seller_name, payroll_store, payroll_store_description, payroll_department_description)
			        CASE WHEN WORKER_STATUS = 'TERMINATED' THEN 'TERMINATED' 
			        		WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED'
			        		ELSE cc.seller_id END AS seller_id,
			        		
					CASE WHEN WORKER_STATUS = 'TERMINATED' THEN 'TERMINATED' 
							WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED'
							ELSE hr.employee_name END AS seller_name,
					
			        CASE WHEN (	
			       			(a.day_date>= coalesce(hr.termination_date, date '9999-12-31') 
				   			and a.day_date <= coalesce(hr.latest_hire_date, date '9999-12-31'))
						or
							 (a.day_date >= coalesce(hr.termination_date, date '9999-12-31') 
							 and hr.termination_date> hr.latest_hire_date)
						)THEN 'TERMINATED'
						WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED'
						WHEN cc.seller_id is null then null 
						ELSE 'ACTIVE' END AS worker_status,
								--use case 1) seller terminated (NULL latest_hire_date) --> post termination date, shows as TERMINATED
								--use case 2) seller terminated and later re-hired --> TERMINATED when in between termination and latest_hire_dates
								--use case 3) seller never terminated (NULL termination_date) ---> as long as seller in HR tables, always ACTIVE*/
					
					case when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_store end as payroll_store,
			        CASE when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_store_description end AS payroll_store_description,
			        CASE when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_department_description END AS payroll_department_description			--JUST ADDED IN WITHOUT CAUSING DUPLICATION, CONSIDER ADDING INTO VIZ
			        	        
	                
	                ---GROUP BY ACOMPANIED BY NEW PRIMARY INDEX AND PARTITION SPED UP QUERY
	                , max(cc.seller_history) as seller_history,
	                max(cc.clienteling_history)as clienteling_history,
	                max(case when first_clienteling_consent_dt between a.min_day_date and a.max_day_date 
	                				and clienteling_history = 'NEW TO CLIENTELING'
	                				then 1 else 0 end) as new_to_clienteling,
	                
	                max(case when a.day_date = cc.opt_in_date_pst then 1 else 0 end)as optedInToday,
	                max(case when a.day_date = cc.opt_out_date_pst then 1 else 0 end)as optedOutToday,
	                
	                max(case when a.day_date >= cc.first_seller_consent_dt then 1 else 0 end)as consented_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+30 then 1 else 0 end)as consented_1mo_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+90 then 1 else 0 end)as consented_3mo_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+180 then 1 else 0 end)as consented_6mo_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+365 then 1 else 0 end)as consented_1yr_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+730 then 1 else 0 end)as consented_2yr_wSeller,
	                
	                max(case when a.day_date >= cc.first_clienteling_consent_dt then 1 else 0 end)as consented_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+30 then 1 else 0 end)as consented_1mo_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+90 then 1 else 0 end)as consented_3mo_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+180 then 1 else 0 end)as consented_6mo_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+365 then 1 else 0 end)as consented_1yr_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+730 then 1 else 0 end)as consented_2yr_wClienteling

	        from cal_dates a 
	       --sel * from t3dl_ace_corp.jiqr_consentedCustomer_dat where acp_id = 'amp::80de0a85-71dc-33da-9436-a4cdca0a32d9'
	        join T2DL_DAS_CLIENTELING.consented_customer_relationship_hist cc on a.day_date between cc.opt_in_date_pst and cc.opt_out_date_pst
	       
	        left join hr_range hr on lpad(cc.seller_id,15,'0') = hr.worker_number
	        						and a.day_date between hr.eff_begin_date and hr.eff_end_date
	        						
	        where cc.acp_id is not null 
	        		--BREAK INTO PART 1
	        		and (a.fm_range like '%2023%' or a.fm_range like '%2025%')
			group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
			
) WITH DATA PRIMARY INDEX (acp_id, seller_id)
PARTITION BY RANGE_N(min_day_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;



--drop table consented_daily_pt2;
CREATE  MULTISET VOLATILE TABLE consented_daily_pt2 AS (


			SELECT 
	                cc.acp_id,
					--a.day_date,
					a.fm_range,
					a.min_day_date, a.max_day_date,
	                a.fiscal_month, a.fiscal_quarter,a.fiscal_year,
	                --a.ly_day_date,
	                
	               --CUSTOMER ATTRIBUTES
	                cc.first_seller_consent_dt,
	                cc.first_clienteling_consent_dt,
	                
				   --V1 SELLER attributes (worker_status, seller_id, seller_name, payroll_store, payroll_store_description, payroll_department_description)
			        --cc.seller_id,
	                /*--validation start
	                sel distinct a.day_date, hr.termination_date, hr.latest_hire_date, case WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED' end payroll, 
	                	cc.seller_id,
	                --validation update end*/
	                --V2 SELLER attributes (worker_status, seller_id, seller_name, payroll_store, payroll_store_description, payroll_department_description)
			        CASE WHEN WORKER_STATUS = 'TERMINATED' THEN 'TERMINATED' 
			        		WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED'
			        		ELSE cc.seller_id END AS seller_id,
			        		
					CASE WHEN WORKER_STATUS = 'TERMINATED' THEN 'TERMINATED' 
							WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED'
							ELSE hr.employee_name END AS seller_name,
					
			        CASE WHEN (	
			       			(a.day_date>= coalesce(hr.termination_date, date '9999-12-31') 
				   			and a.day_date <= coalesce(hr.latest_hire_date, date '9999-12-31'))
						or
							 (a.day_date >= coalesce(hr.termination_date, date '9999-12-31') 
							 and hr.termination_date> hr.latest_hire_date)
						)THEN 'TERMINATED'
						WHEN hr.payroll_store_description like '%CLOSED%' then 'CLOSED'
						WHEN cc.seller_id is null then null 
						ELSE 'ACTIVE' END AS worker_status,
								--use case 1) seller terminated (NULL latest_hire_date) --> post termination date, shows as TERMINATED
								--use case 2) seller terminated and later re-hired --> TERMINATED when in between termination and latest_hire_dates
								--use case 3) seller never terminated (NULL termination_date) ---> as long as seller in HR tables, always ACTIVE*/
					
					case when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_store end as payroll_store,
			        CASE when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_store_description end AS payroll_store_description,
			        CASE when hr.payroll_store_description like '%CLOSED%' then 'CLOSED' else hr.payroll_department_description END AS payroll_department_description			--JUST ADDED IN WITHOUT CAUSING DUPLICATION, CONSIDER ADDING INTO VIZ
			        	        
	                
	                ---GROUP BY ACOMPANIED BY NEW PRIMARY INDEX AND PARTITION SPED UP QUERY
	                , max(cc.seller_history) as seller_history,
	                max(cc.clienteling_history)as clienteling_history,
	                max(case when first_clienteling_consent_dt between a.min_day_date and a.max_day_date 
	                				and clienteling_history = 'NEW TO CLIENTELING'
	                				then 1 else 0 end) as new_to_clienteling,
	                
	                max(case when a.day_date = cc.opt_in_date_pst then 1 else 0 end)as optedInToday,
	                max(case when a.day_date = cc.opt_out_date_pst then 1 else 0 end)as optedOutToday,
	                
	                max(case when a.day_date >= cc.first_seller_consent_dt then 1 else 0 end)as consented_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+30 then 1 else 0 end)as consented_1mo_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+90 then 1 else 0 end)as consented_3mo_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+180 then 1 else 0 end)as consented_6mo_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+365 then 1 else 0 end)as consented_1yr_wSeller,
	                max(case when a.day_date >= cc.first_seller_consent_dt+730 then 1 else 0 end)as consented_2yr_wSeller,
	                
	                max(case when a.day_date >= cc.first_clienteling_consent_dt then 1 else 0 end)as consented_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+30 then 1 else 0 end)as consented_1mo_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+90 then 1 else 0 end)as consented_3mo_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+180 then 1 else 0 end)as consented_6mo_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+365 then 1 else 0 end)as consented_1yr_wClienteling,
	                max(case when a.day_date >= cc.first_clienteling_consent_dt+730 then 1 else 0 end)as consented_2yr_wClienteling

	        from cal_dates a 
	       --sel * from t3dl_ace_corp.jiqr_consentedCustomer_dat where acp_id = 'amp::80de0a85-71dc-33da-9436-a4cdca0a32d9'
	        join T2DL_DAS_CLIENTELING.consented_customer_relationship_hist cc on a.day_date between cc.opt_in_date_pst and cc.opt_out_date_pst
	       
	        left join hr_range hr on lpad(cc.seller_id,15,'0') = hr.worker_number
	        						and a.day_date between hr.eff_begin_date and hr.eff_end_date
	        						
	        where cc.acp_id is not null 
	        --BREAK INTO PART 2
	        	and not (a.fm_range like '%2023%' or a.fm_range like '%2025%')
			group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
			
) WITH DATA PRIMARY INDEX (acp_id, seller_id)
PARTITION BY RANGE_N(min_day_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;


--DROP TABLE consented_daily;
CREATE  MULTISET VOLATILE TABLE consented_daily AS (
		--UNIONED ROWS FOR CONSENTED SELLER AND SALES FROM NON-CONSENTED
		sel * from consented_daily_pt1
		union
		sel * from consented_daily_pt2
) WITH DATA PRIMARY INDEX (acp_id, seller_id)
PARTITION BY RANGE_N(min_day_date between date '2019-01-01' and date '2031-12-31' each interval '1' day)
ON COMMIT PRESERVE ROWS;



COLLECT STATISTICS COLUMN (acp_id, seller_id, fm_range) ON consented_daily;

/*****************************************************************
 * 
 * 		MERGING TRANSACTIONS/TRIPS AND CONSENTED CUSTOMER DATA
 * 
 * 
 *****************************************************************/

--STEP 5: PREP SET OF SELLERS WHERE CUSTOMER SALES GO (TO INCLUDE NULL NON-COMMISSIONED SALES)
--@ fm_range||ACP_ID||SELLER_ID LEVEL
--drop table customer_date_list;
CREATE  MULTISET VOLATILE TABLE customer_date_list AS (
		--UNIONED ROWS FOR CONSENTED SELLER AND SALES FROM NON-CONSENTED
		sel distinct
			fm_range, fiscal_month, fiscal_quarter, fiscal_year, acp_id,
					seller_id, seller_name, 
					CAST(worker_status AS VARCHAR(30)) as worker_status, 
					payroll_store, 
					cast(payroll_store_description as varchar(100)) as payroll_store_description, 
					cast(payroll_department_description as varchar(100)) as payroll_department_description
			from consented_daily 
		union
		sel distinct 
			a.fm_range, a.fiscal_month, a.fiscal_quarter, a.fiscal_year, a.acp_id,
					a.seller_id, a.seller_name, 
					CAST(worker_status AS VARCHAR(30)) as worker_status, 
					a.payroll_store, 
					cast(a.payroll_store_description as varchar(100)) as payroll_store_description, 
					cast(a.payroll_department_description as varchar(100)) as payroll_department_description
			from tyly_sales_weekly a 
			join (sel distinct acp_id, first_clienteling_consent_dt from consented_daily) b on a.acp_id = b.acp_id

) WITH DATA PRIMARY INDEX (acp_id) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (acp_id, seller_id) ON customer_date_list;



--STEP 6: APPEND CONSENT DATA TO SALE DATA
--			ADD CUSTOMER LEVEL SALES DATA (NOT JUST SELLER-LEVEL)
--@ fm_range||ACP_ID||SELLER_ID LEVEL
--drop table consented_trx_acp;
CREATE  MULTISET VOLATILE TABLE consented_trx_acp AS (		

	sel distinct
		   a.acp_id,

		   a.fm_range,	
		   max(ccc.min_day_date) over (partition by a.fm_range) as min_day_date,
		   max(ccc.max_day_date) over (partition by a.fm_range) as max_day_date,
		   a.fiscal_month,a.fiscal_quarter,a.fiscal_year,
			
		 --NEW CONNECTION attributes (acp_id, seller level- NULL FOR NON CONSENTED SELLER)
		   a.seller_id, 
		   max(a.seller_name) over (partition by a.seller_id, a.fm_range) as seller_name, 
		   a.worker_status,
		   a.payroll_store,
		   a.payroll_store_description,
		   a.payroll_department_description, 
		   
		   --CUSTOMER SPECIFIC (CAN ADD RETURN RATE ATTRIBUTE HERE) - currently set for all consented customers! 
		   -- NEED TO TIE THESE SALES TO SELLER OF ATTRIBUTION!!!
		   ccc.first_seller_consent_dt as first_seller_consent_dt, 
		   ccc.first_clienteling_consent_dt as first_clienteling_consent_dt,
		   ccc.seller_history as seller_history, 				--prioritize NEW reln to EXISTING
		   ccc.clienteling_history as clienteling_history, 
		   ccc.new_to_clienteling as new_to_clienteling,
		   
		 --CONSENTED CUSTOMER COUNTS
		   case when coalesce(ccc.worker_status, s.worker_status) <> 'TERMINATED' then 1 else 0 end as hasNonTerminatedSeller_ind,
		   
	     -- CUSTOMER-SELLER SALE attributes
		   coalesce(s.seller_FLS_trips,0) as seller_FLS_trips,
	       coalesce(s.seller_NCOM_trips,0) as seller_NCOM_trips,
	       coalesce(s.seller_sb_NCOM_trips,0) as seller_sb_NCOM_trips, 
	        
	       coalesce(s.seller_gross_FLS_spend,0) as seller_gross_FLS_spend,
	       coalesce(s.seller_gross_NCOM_spend,0) as seller_gross_NCOM_spend,	       
	       coalesce(s.seller_sb_gross_NCOM_spend,0) as seller_sb_gross_NCOM_spend,
	       
	     -- CUSTOMER- TRIP ATTRB SELLER SALE ATTRIBUTES
		   coalesce(s.TS_FLS_trips,0) as TS_FLS_trips,
	       coalesce(s.TS_NCOM_trips,0) as TS_NCOM_trips,
	       coalesce(s.TS_sb_NCOM_trips,0) as TS_sb_NCOM_trips, 
	        
	       coalesce(s.TS_gross_FLS_spend,0) as TS_gross_FLS_spend,
	       coalesce(s.TS_gross_NCOM_spend,0) as TS_gross_NCOM_spend,	       
	       coalesce(s.TS_sb_gross_NCOM_spend,0) as TS_sb_gross_NCOM_spend,
	        
	     -- CUSTOMER-STORE LEVEL ATTRIBUTES
	       sum(coalesce(s.TS_FLS_trips,0)) over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_FLS_trips,
	       sum(coalesce(s.TS_NCOM_trips,0)) over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_NCOM_trips,
	       sum(coalesce(s.TS_sb_NCOM_trips,0)) over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_sb_NCOM_trips, 
	       
	       sum(case when UPPER(a.seller_id) NOT IN ('NA', 'TERMINATED', 'CLOSED','NON-CONSENTED SELLER') and a.seller_id is not null then s.TS_FLS_trips end) 
	       															over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_FLS_trips_Connection,
	       sum(case when UPPER(a.seller_id) NOT IN ('NA', 'TERMINATED', 'CLOSED','NON-CONSENTED SELLER') and a.seller_id is not null then s.TS_NCOM_trips end) 
	       															over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_NCOM_trips_Connection,
	       sum(case when UPPER(a.seller_id) NOT IN ('NA', 'TERMINATED', 'CLOSED','NON-CONSENTED SELLER') and a.seller_id is not null then s.TS_sb_NCOM_trips end) 
	       															over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_sb_NCOM_trips_Connection, 
	       
	       sum(case when UPPER(a.seller_id) ='NON-CONSENTED SELLER' then s.TS_FLS_trips end) 
	       									over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_FLS_trips_NonConnection,
	       sum(case when UPPER(a.seller_id) ='NON-CONSENTED SELLER' then s.TS_NCOM_trips end) 
	       									over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_NCOM_trips_NonConnection,
	       sum(case when UPPER(a.seller_id) ='NON-CONSENTED SELLER' then s.TS_sb_NCOM_trips end) 
	       									over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_sb_NCOM_trips_NonConnection, 
	       									
	       sum(case when UPPER(a.seller_id) ='NA' or a.seller_id is null then s.TS_FLS_trips end) 
	       									over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_FLS_trips_woSeller,
	       sum(case when UPPER(a.seller_id) ='NA' or a.seller_id is null then s.TS_NCOM_trips end) 
	       									over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_NCOM_trips_woSeller,
	       sum(case when UPPER(a.seller_id) ='NA' or a.seller_id is null then s.TS_sb_NCOM_trips end) 
	       									over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_sb_NCOM_trips_woSeller, 
	       									
	       sum(coalesce(s.TS_gross_FLS_spend,0)) over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_gross_FLS_spend,	        
	       sum(coalesce(s.TS_gross_NCOM_spend,0)) over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_gross_NCOM_spend,
	       sum(coalesce(s.TS_sb_gross_NCOM_spend,0)) over (partition by a.fm_range, a.acp_id, coalesce(ccc.payroll_store, s.payroll_store)) as store_sb_gross_NCOM_spend,
	       
	     -- CUSTOMER-MONTH LEVEL ATTRIBUTES
	       sum(coalesce(s.TS_FLS_trips,0)) over (partition by a.fm_range, a.acp_id) as mo_FLS_trips,
	       sum(coalesce(s.TS_NCOM_trips,0)) over (partition by a.fm_range, a.acp_id) as mo_NCOM_trips,
	       sum(coalesce(s.TS_sb_NCOM_trips,0)) over (partition by a.fm_range, a.acp_id) as mo_sb_NCOM_trips, 
	       
	       sum(coalesce(s.TS_gross_FLS_spend,0)) over (partition by a.fm_range, a.acp_id) as mo_gross_FLS_spend,	        
	       sum(coalesce(s.TS_gross_NCOM_spend,0)) over (partition by a.fm_range, a.acp_id) as mo_gross_NCOM_spend,
	       sum(coalesce(s.TS_sb_gross_NCOM_spend,0)) over (partition by a.fm_range, a.acp_id) as mo_sb_gross_NCOM_spend,
	       
	        
		   ccc.optedInToday, 
		   ccc.optedOutToday,  
		   
		   ccc.consented_wSeller, 							---THIS SHOULD CONTAIN TERMINATED
		   ccc.consented_1mo_wSeller,  
		   ccc.consented_3mo_wSeller,   
		   ccc.consented_6mo_wSeller,  
		   ccc.consented_1yr_wSeller,  
		   ccc.consented_2yr_wSeller,  
		   
		   ccc.consented_wClienteling,  
		   ccc.consented_1mo_wClienteling, 
		   ccc.consented_3mo_wClienteling, 
		   ccc.consented_6mo_wClienteling, 
		   ccc.consented_1yr_wClienteling, 
		   ccc.consented_2yr_wClienteling  

	from customer_date_list a	 			
	left join consented_daily ccc on a.fm_range = ccc.fm_range		
										and a.acp_id = ccc.acp_id
										and coalesce(a.seller_id,'na') = coalesce(ccc.seller_id,'na')
	    								and coalesce(a.worker_status,'na') = coalesce(ccc.worker_status,'na')
	    								and coalesce(a.payroll_store,'na') = coalesce(ccc.payroll_store,'na')
	    								and coalesce(a.payroll_department_description,'na') = coalesce(ccc.payroll_department_description,'na')
																										
    left join tyly_sales_weekly s on a.fm_range = s.fm_range		
	    								and a.acp_id = s.acp_id
	    								and coalesce(a.seller_id,'na') = coalesce(s.seller_id,'na')
	    								and coalesce(a.worker_status,'na') = coalesce(s.worker_status,'na')
	    								and coalesce(a.payroll_store,'na') = coalesce(s.payroll_store,'na')
	    								and coalesce(a.payroll_department_description,'na') = coalesce(s.payroll_department_description,'na')

) WITH DATA PRIMARY INDEX (acp_id) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (seller_id, acp_id, fm_range) ON consented_trx_acp;



/*****************************************************************
 * 
 * 		PULLING CUSTOMER ATTRIBUTES (CURRENT AND HISTORICAL
 * 
 * 
 *****************************************************************/


--drop table rtdf;
CREATE  MULTISET VOLATILE TABLE rtdf AS (
			sel distinct dtl.acp_id,
		        --dtl.global_tran_id,				
		        --dtl.line_item_seq_num,
		        coalesce(dtl.order_date,dtl.tran_date) as sale_date,	
		        
		        --dtl.line_item_quantity as item_cnt,
		        --dtl.line_net_usd_amt as gross_amt,
		        
		        case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
		              and dtl.line_item_net_amt_currency_code = 'CAD' then 867
		            when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp'
		              and dtl.line_item_net_amt_currency_code = 'USD' then 808									
		            else dtl.intent_store_num end as store_num,
		           
		        case when dtl.commission_slsprsn_num is not null 
	        		and dtl.commission_slsprsn_num not in ('2079333', '2079334', '0000') 
	        		then dtl.commission_slsprsn_num else null end as commission_slsprsn_num,
	        		
	        	dtl.item_source
		    
		    from prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
		    left join restaurant_dept_lookup r on coalesce(dtl.merch_dept_num,-1*dtl.intent_store_num)=r.dept_num
		    --join dates d on dtl.business_day_date between d.min_date-365 and d.max_date+30
		    where trim(coalesce(dtl.upc_num,'-1'))<>'-1'  --Filters out Non-Merch or Restaurant (RPOS)
				and dtl.acp_id is not null
				and dtl.line_net_usd_amt > 0 
				and coalesce(dtl.nonmerch_fee_code,'na') <> '6666'	--FILTERING OUT PURCHASES MADE TOWARDS BUYING GIFT CARD
				and sale_date between (sel min_date-365 from dates) and (sel max_date+30 from dates)
				and dtl.business_day_date between (sel min_date-365 from dates) and (sel max_date+30 from dates)
				and r.dept_num is null
				
) with data primary index(acp_id, sale_date) 
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (sale_date, acp_id) ON rtdf;


---STEP 8: PULL CUSTOMER-LEVEL CHURN ATTRIBUTES (fp_churned_ind, allConnections_churned_ind)

--drop table tyly_sales_365hist1;
CREATE  MULTISET VOLATILE TABLE tyly_sales_365hist1 AS (
		sel 
			c.acp_id, c.fm_range, c.min_day_date, c.max_day_date,
			--current enrollment status
			max(case when worker_status = 'ACTIVE' then 1 
					when worker_status = 'TERMINATED' then 0 else null end) as hasNonTerminatedSeller_ind,
					
			max(case when worker_status = 'ACTIVE' and consented_1yr_wSeller = 1 then 1 
					else 0 end) as has1yrNonTerminatedSeller_ind,
			
			max(c.consented_1yr_wClienteling) as consented_1yr_wClienteling, 
			
			max(c.new_to_clienteling) as new_to_clienteling,
			
			--365 day history
	        --max(case when datSel.commission_slsprsn_num IS NOT NULL then 'Y' else 'N' end) as sale_w_consented_seller,
			max(case when dat365.acp_id is not null then 1 else 0 end) AS fp_sale,
			max(case when str.business_unit_desc = 'FULL LINE' then 1 else 0 end) as fls_sale,
			max(case when str.business_unit_desc = 'N.COM' then 1 else 0 end) as ncom_sale
		--sel distinct c.acp_id, c.fm_range, c.min_day_date, c.max_day_date  --from rtdf;		--302.7M
	    from consented_trx_acp c
											
	    --PREPARE TO MAKE THIS LEFT JOIN FOR ALL CUSTOMERS
		left join rtdf dat365 on dat365.acp_id = c.acp_id
									and dat365.sale_date between c.max_day_date-365 and c.max_day_date		--153.5M
		
		left join prd_nap_usr_vws.store_dim as str  on dat365.store_num = str.store_num
											and str.business_unit_desc in ('FULL LINE','N.COM')		--840M	--dropped to 83,620,623
		
		group by 1,2,3,4
		
) with data primary index(acp_id, fm_range) 
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (ACP_ID, fm_range) ON tyly_sales_365hist1;


--drop table tyly_sales_365hist2;
CREATE  MULTISET VOLATILE TABLE tyly_sales_365hist2 AS (
		sel 
			c.acp_id, c.fm_range, c.min_day_date, c.max_day_date,
			
			--365 day history
	        max(case when datSel.commission_slsprsn_num IS NOT NULL then 'Y' else 'N' end) as sale_w_consented_seller

	    from consented_trx_acp c 
											
	   
		left join rtdf datSel on datSel.acp_id = c.acp_id
									and datSel.sale_date between c.max_day_date-365 and c.max_day_date
									AND coalesce(c.seller_id,'na') = datSel.commission_slsprsn_num		--840M
		
		group by 1,2,3,4
		
) with data primary index(acp_id, fm_range) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (ACP_ID, fm_range) ON tyly_sales_365hist2;

--drop table tyly_sales_365hist;
CREATE  MULTISET VOLATILE TABLE tyly_sales_365hist AS (
		sel distinct
			c.acp_id, c.fm_range, c.min_day_date, c.max_day_date,
			
			--365 day history
	        
			hasNonTerminatedSeller_ind, has1yrNonTerminatedSeller_ind, consented_1yr_wClienteling, new_to_clienteling,
			sale_w_consented_seller, fp_sale, fls_sale, ncom_sale

	    from tyly_sales_365hist1 c 
											
	   
		left join tyly_sales_365hist2 b on c.acp_id = b.acp_id
									and c.fm_range = b.fm_range
		
) with data primary index(acp_id, fm_range) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (ACP_ID, fm_range) ON tyly_sales_365hist;


--drop table pre_attributes;
create multiset volatile table pre_attributes as (
        select a.*, 
            max(pmt.ph_preference_value) as phone_marketability, 
			max(pmt.em_preference_value) as email_marketability
            --min(pmt.rewards_level) as loyalty_level				--IF DESIRED, WILL NEED TO ADD TO CONSENTED CUSTOMER DATA SOURCE jiqr_consentedCustomer_dat

			from tyly_sales_365hist a
            left join (sel distinct acp_id, email_pref_cur as em_preference_value, phone_pref_cur as ph_preference_value 
            		from T2DL_DAS_CLIENTELING.consented_customer_relationship_hist	) pmt     on a.acp_id = pmt.acp_id
            		--from t3dl_ace_corp.jiqr_consentedCustomer_dat) pmt     on a.acp_id = pmt.acp_id 
            group by 1,2,3,4,5,6,7,8,9,10,11,12
        
    )with data primary index(acp_id)on commit preserve rows;
   

--continue here!!
---STEP 8: PULL CUSTOMER-LEVEL CHURN ATTRIBUTES (STILL NEED TO VALIDATE, & takes longggg)
--@fm_range||ACP_ID LEVEL
--drop table customer_attrb;
CREATE  MULTISET VOLATILE TABLE customer_attrb AS (
	sel distinct 
		acp_id, 
		fm_range, 
		min_day_date, 
		max_day_date,
	
		consented_1yr_wClienteling,
		
		case when hasNonTerminatedSeller_ind = 0 then 1 
			when hasNonTerminatedSeller_ind = 1 then 0 end as hasOnlyTerminatedSeller_ind,		--if you have an active seller, you dont only have terminated
			
		has1yrNonTerminatedSeller_ind,
		new_to_clienteling,
		
		--boughtFromSB, boughtFromConnection,
		case when sale_w_consented_seller = 'N' and has1yrNonTerminatedSeller_ind=1 then 1 else 0 end as allConnections_churned_ind,
		
		case when fp_sale = 0 and has1yrNonTerminatedSeller_ind=1 then 1 else 0 end as FP_churned_ind,
		case when fls_sale = 0 and has1yrNonTerminatedSeller_ind=1 then 1 else 0 end as FLS_churned_ind,
		case when ncom_sale = 0 and has1yrNonTerminatedSeller_ind=1 then 1 else 0 end as NCOM_churned_ind,
		
		phone_marketability,
		email_marketability
		
	 	FROM pre_attributes b 

) with data primary index(acp_id, fm_range) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (ACP_ID, fm_range) ON customer_attrb;

/*********************************************************************
 * 
 * 			ALL UP AGGREGATIONS BY MONTH & BY STORE
 * 				THESE ALSO SERVE AS DENOMINATORS
 * 
 *********************************************************************/

--STEP 9:  PULLING STORE-LEVEL ATTRIBUTES LATER APPEND
--@fm_range||PAYROLL_STORE LEVEL
--drop table store_attrb;
CREATE  MULTISET VOLATILE TABLE store_attrb AS (
	sel b.fm_range, 
		b.payroll_store,
		
		--appended
		b.cnt_OptedInToday as store_tot_cnt_OptedInToday,
		b.cnt_OptedOutToday as store_tot_cnt_OptedOutToday,
		b.cnt_Consented_wSeller as store_tot_cnt_Consented_wSeller,
		b.cnt_Consented_1mo_wSeller as store_tot_cnt_Consented_1mo_wSeller,			--do we want this to be of Total or value in OnlyTerminatedSeller vs HasActiveSeller?
		b.cnt_Consented_3mo_wSeller as store_tot_cnt_Consented_3mo_wSeller,
		b.cnt_Consented_6mo_wSeller as store_tot_cnt_Consented_6mo_wSeller,
		b.cnt_Consented_1yr_wSeller as store_tot_cnt_Consented_1yr_wSeller,
		b.cnt_Consented_2yr_wSeller as store_tot_cnt_Consented_2yr_wSeller,
		b.cnt_Consented_1mo_wClienteling as store_tot_cnt_Consented_1mo_wClienteling,
		b.cnt_Consented_3mo_wClienteling as store_tot_cnt_Consented_3mo_wClienteling,
		b.cnt_Consented_6mo_wClienteling as store_tot_cnt_Consented_6mo_wClienteling,
		b.cnt_Consented_1yr_wClienteling as store_tot_cnt_Consented_1yr_wClienteling,
		b.cnt_Consented_2yr_wClienteling as store_tot_cnt_Consented_2yr_wClienteling,
			
		--AGG SALES (OVER CUSTOMER, REPORTING ON DAY||STORE LEVEL)
		sum(a.hasOnlyTerminatedSeller_ind) as store_tot_hasOnlyTerminatedSeller_ind,
		sum(a.new_to_clienteling) as store_tot_new_to_clienteling,
		sum(a.has1yrNonTerminatedSeller_ind) as store_tot_has1yrNonTerminatedSeller_ind,
		sum(a.allConnections_churned_ind) as store_tot_allConnections_churned_ind,
		sum(a.FP_churned_ind) as store_tot_FP_churned_ind,
		--sum(a.boughtFromConnection) as store_tot_boughtFromConnection,
		--sum(a.boughtFromSB) as store_tot_boughtFromSB,
		sum(case when a.phone_marketability='Y' then 1 else 0 end) as store_tot_phoneMarketable_Y,
		sum(case when a.phone_marketability='N' then 1 else 0 end) as store_tot_phoneMarketable_N,
		sum(case when a.email_marketability='Y' then 1 else 0 end) as store_tot_emailMarketable_Y,
		sum(case when a.email_marketability='N' then 1 else 0 end) as store_tot_emailMarketable_N,
		
		sum(a.store_fls_trips) as store_tot_fls_trips,
		sum(a.store_ncom_trips) as store_tot_ncom_trips,
		sum(a.store_sb_ncom_trips) as store_tot_sb_ncom_trips,
		
		sum(a.store_fls_trips_Connection) as store_tot_fls_trips_Connection,
		sum(a.store_ncom_trips_Connection) as store_tot_ncom_trips_Connection,
		sum(a.store_sb_ncom_trips_Connection) as store_tot_sb_ncom_trips_Connection,
		
		sum(a.store_fls_trips_NonConnection) as store_tot_fls_trips_NonConnection,
		sum(a.store_ncom_trips_NonConnection) as store_tot_ncom_trips_NonConnection,
		sum(a.store_sb_ncom_trips_NonConnection) as store_tot_sb_ncom_trips_NonConnection,
		
		sum(a.store_fls_trips_woSeller) as store_tot_fls_trips_woSeller,
		sum(a.store_ncom_trips_woSeller) as store_tot_ncom_trips_woSeller,
		sum(a.store_sb_ncom_trips_woSeller) as store_tot_sb_ncom_trips_woSeller,
		
		sum(a.store_gross_fls_spend) as store_tot_gross_fls_spend,
		sum(a.store_gross_ncom_spend) as store_tot_gross_ncom_spend,
		sum(a.store_sb_gross_ncom_spend) as store_tot_sb_gross_ncom_spend
		
	--AGG CUSTOMER COUNTS
	from 
		(sel a.payroll_store, 
				--5 appended customer attributes (customer-day level)
				a.fm_range,
				a.acp_id,
				b.consented_1yr_wClienteling,
				b.has1yrNonTerminatedSeller_ind,
				b.hasOnlyTerminatedSeller_ind,
				b.new_to_clienteling,
				b.allConnections_churned_ind,
				b.FP_churned_ind,
				--b.boughtFromConnection,
				--b.boughtFromSB,
				b.phone_marketability,
				b.email_marketability,
				
				--SALES AT CUSTOMER-STORE LEVEL
				avg(a.store_fls_trips) store_fls_trips,
				avg(a.store_ncom_trips) store_ncom_trips,
				avg(a.store_sb_ncom_trips) store_sb_ncom_trips,
				
				avg(a.store_fls_trips_Connection) store_fls_trips_Connection,
				avg(a.store_ncom_trips_Connection) store_ncom_trips_Connection,
				avg(a.store_sb_ncom_trips_Connection) store_sb_ncom_trips_Connection,
				
				avg(a.store_fls_trips_NonConnection) store_fls_trips_NonConnection,
				avg(a.store_ncom_trips_NonConnection) store_ncom_trips_NonConnection,
				avg(a.store_sb_ncom_trips_NonConnection) store_sb_ncom_trips_NonConnection,
				
				avg(a.store_fls_trips_woSeller) store_fls_trips_woSeller,
				avg(a.store_ncom_trips_woSeller) store_ncom_trips_woSeller,
				avg(a.store_sb_ncom_trips_woSeller) store_sb_ncom_trips_woSeller,
				
				avg(a.store_gross_fls_spend) store_gross_fls_spend,
				avg(a.store_gross_ncom_spend) store_gross_ncom_spend,
				avg(a.store_sb_gross_ncom_spend) store_sb_gross_ncom_spend
				
		from consented_trx_acp a ---consented_trx_out a					sel count(distinct acp_id) from consented_trx_acp where fm_range = '(2024-02-04 - 2024-03-02)'
		left join customer_attrb b on a.fm_range = b.fm_range
										and a.acp_id = b.acp_id
		group by 1,2,3,4,5,6,7,8,9,10,11
		) a 
	
	left join 
		(sel fm_range, payroll_store,
			count(distinct case when OptedInToday=1 then acp_id end) as cnt_OptedInToday,
			count(distinct case when OptedOutToday=1 then acp_id end) as cnt_OptedOutToday,
			count(distinct case when consented_wSeller=1 then acp_id end) as cnt_Consented_wSeller,	
			count(distinct case when consented_1mo_wSeller=1 then acp_id end) as cnt_Consented_1mo_wSeller,
			count(distinct case when consented_3mo_wSeller=1 then acp_id end) as cnt_Consented_3mo_wSeller,
			count(distinct case when consented_6mo_wSeller=1 then acp_id end) as cnt_Consented_6mo_wSeller,
			count(distinct case when consented_1yr_wSeller=1 then acp_id end) as cnt_Consented_1yr_wSeller,
			count(distinct case when consented_2yr_wSeller=1 then acp_id end) as cnt_Consented_2yr_wSeller,
			count(distinct case when consented_1mo_wClienteling=1 then acp_id end) as cnt_Consented_1mo_wClienteling,
			count(distinct case when consented_3mo_wClienteling=1 then acp_id end) as cnt_Consented_3mo_wClienteling,
			count(distinct case when consented_6mo_wClienteling=1 then acp_id end) as cnt_Consented_6mo_wClienteling,
			count(distinct case when consented_1yr_wClienteling=1 then acp_id end) as cnt_Consented_1yr_wClienteling,
			count(distinct case when consented_2yr_wClienteling=1 then acp_id end) as cnt_Consented_2yr_wClienteling
		from consented_trx_acp  
		group by 1,2
		) b on a.fm_range = b.fm_range
				and coalesce(a.payroll_store,'unknown') = coalesce(b.payroll_store,'unknown')
	
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 --,16,17,18

) WITH DATA PRIMARY INDEX (fm_range, payroll_store) 
ON COMMIT PRESERVE ROWS;

collect statistics column (fm_range, payroll_store) on store_attrb;


--STEP 10: PULLING MONTH-LEVEL ATTRIBUTES LATER APPEND
--@fm_range LEVEL
--drop table month_attrb;
CREATE  MULTISET VOLATILE TABLE month_attrb AS (
	sel 	b.fm_range, 
			
			--appended
			b.cnt_OptedInToday as month_tot_cnt_OptedInToday,
			b.cnt_OptedOutToday as month_tot_cnt_OptedOutToday,
			b.cnt_Consented_wSeller as month_tot_cnt_Consented_wSeller,
			b.cnt_Consented_1mo_wSeller as month_tot_cnt_Consented_1mo_wSeller,
			b.cnt_Consented_3mo_wSeller as month_tot_cnt_Consented_3mo_wSeller,
			b.cnt_Consented_6mo_wSeller as month_tot_cnt_Consented_6mo_wSeller,
			b.cnt_Consented_1yr_wSeller as month_tot_cnt_Consented_1yr_wSeller,
			b.cnt_Consented_2yr_wSeller as month_tot_cnt_Consented_2yr_wSeller,
			b.cnt_Consented_1mo_wClienteling as month_tot_cnt_Consented_1mo_wClienteling,
			b.cnt_Consented_3mo_wClienteling as month_tot_cnt_Consented_3mo_wClienteling,
			b.cnt_Consented_6mo_wClienteling as month_tot_cnt_Consented_6mo_wClienteling,
			b.cnt_Consented_1yr_wClienteling as month_tot_cnt_Consented_1yr_wClienteling,
			b.cnt_Consented_2yr_wClienteling as month_tot_cnt_Consented_2yr_wClienteling,
			
		--AGG SALES (OVER CUSTOMER, REPORTING ON DAY||STORE LEVEL)
		sum(a.hasOnlyTerminatedSeller_ind) as month_tot_hasOnlyTerminatedSeller_ind,
		sum(a.new_to_clienteling) as month_tot_new_to_clienteling,
		sum(a.has1yrNonTerminatedSeller_ind) as month_tot_has1yrNonTerminatedSeller_ind,
		sum(a.allConnections_churned_ind) as month_tot_allConnections_churned_ind,
		sum(a.FP_churned_ind) as month_tot_FP_churned_ind,
		--sum(a.boughtFromConnection) as month_tot_boughtFromConnection,
		--sum(a.boughtFromSB) as month_tot_boughtFromSB,
		sum(case when a.phone_marketability='Y' then 1 else 0 end) as month_tot_phoneMarketable_Y,
		sum(case when a.phone_marketability='N' then 1 else 0 end) as month_tot_phoneMarketable_N,
		sum(case when a.email_marketability='Y' then 1 else 0 end) as month_tot_emailMarketable_Y,
		sum(case when a.email_marketability='N' then 1 else 0 end) as month_tot_emailMarketable_N,
		
		sum(a.mo_fls_trips) as month_tot_fls_trips,
		sum(a.mo_ncom_trips) as month_tot_ncom_trips,
		sum(a.mo_sb_ncom_trips) as month_tot_sb_ncom_trips,
		
		sum(a.mo_gross_fls_spend) as month_tot_gross_fls_spend,
		sum(a.mo_gross_ncom_spend) as month_tot_gross_ncom_spend,
		sum(a.mo_sb_gross_ncom_spend) as month_tot_sb_gross_ncom_spend
	--AGG CUSTOMER COUNTS
	from 
		(sel --5 appended customer attributes (customer-day level)
				a.fm_range,
				a.acp_id,
				b.consented_1yr_wClienteling,
				b.has1yrNonTerminatedSeller_ind,
				b.hasOnlyTerminatedSeller_ind,
				b.new_to_clienteling,
				b.allConnections_churned_ind,
				b.FP_churned_ind,
				--b.boughtFromConnection,
				--b.boughtFromSB,
				b.phone_marketability,
				b.email_marketability,
				
				--SALES AT CUSTOMER-STORE LEVEL
				avg(mo_fls_trips) mo_fls_trips,
				avg(mo_ncom_trips) mo_ncom_trips,
				avg(mo_sb_ncom_trips) mo_sb_ncom_trips,
				
				avg(mo_gross_fls_spend) mo_gross_fls_spend,
				avg(mo_gross_ncom_spend) mo_gross_ncom_spend,
				avg(mo_sb_gross_ncom_spend) mo_sb_gross_ncom_spend

		from consented_trx_acp a -- consented_trx_out a	
		
		left join customer_attrb b on a.fm_range = b.fm_range	
										and a.acp_id = b.acp_id
		group by 1,2,3,4,5,6,7,8,9,10
		) a 
	
	left join 
		(sel fm_range,
			count(distinct case when OptedInToday=1 then acp_id end) as cnt_OptedInToday,
			count(distinct case when OptedOutToday=1 then acp_id end) as cnt_OptedOutToday,
			count(distinct case when consented_wSeller=1 then acp_id end) as cnt_Consented_wSeller,
			count(distinct case when consented_1mo_wSeller=1 then acp_id end) as cnt_Consented_1mo_wSeller,
			count(distinct case when consented_3mo_wSeller=1 then acp_id end) as cnt_Consented_3mo_wSeller,
			count(distinct case when consented_6mo_wSeller=1 then acp_id end) as cnt_Consented_6mo_wSeller,
			count(distinct case when consented_1yr_wSeller=1 then acp_id end) as cnt_Consented_1yr_wSeller,
			count(distinct case when consented_2yr_wSeller=1 then acp_id end) as cnt_Consented_2yr_wSeller,
			count(distinct case when consented_1mo_wClienteling=1 then acp_id end) as cnt_Consented_1mo_wClienteling,
			count(distinct case when consented_3mo_wClienteling=1 then acp_id end) as cnt_Consented_3mo_wClienteling,
			count(distinct case when consented_6mo_wClienteling=1 then acp_id end) as cnt_Consented_6mo_wClienteling,
			count(distinct case when consented_1yr_wClienteling=1 then acp_id end) as cnt_Consented_1yr_wClienteling,
			count(distinct case when consented_2yr_wClienteling=1 then acp_id end) as cnt_Consented_2yr_wClienteling
		from consented_trx_acp --consented_trx_out
		group by 1
		) b on a.fm_range = b.fm_range
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

) WITH DATA PRIMARY INDEX (fm_range) 
--PARTITION BY RANGE_N(fm_range between date '2019-01-01' and date '2025-12-31' each interval '1' day, unknown)
ON COMMIT PRESERVE ROWS;

collect statistics column (fm_range) on month_attrb;


/*****************************************************************************
 * 
 * 		MERGING SELLER-LEVEL AND DENOMINATOR STORE & MONTH LEVEL DATA
 * 					(FINAL OUTPUT)
 * 
 * 
 *****************************************************************************/



--drop table outp_pt1;
CREATE  MULTISET VOLATILE TABLE OUTP_PT1 AS (
	sel a.fm_range, 
		a.fiscal_month, a.fiscal_quarter, a.fiscal_year, 
		
		--seller attributes
		coalesce(a.seller_id,'NA') as seller_id,
		--coalesce(a.seller_name, 'NA') as  seller_name,
		coalesce(a.worker_status, 'NA') as  worker_status,
		coalesce(a.payroll_store, 'NA') as  payroll_store,
		coalesce(a.payroll_store_description, 'NA') as  payroll_store_description,
		coalesce(a.payroll_department_description, 'NA') as  payroll_department_description,		--10 fields so far		2.14.24
	
		--AGGREGATED TO DATE LEVEL			--15 when cut down
		d.month_tot_cnt_OptedInToday, 
		d.month_tot_cnt_OptedOutToday, 
		d.month_tot_cnt_Consented_wSeller, 
		
		d.month_tot_hasOnlyTerminatedSeller_ind, 
		d.month_tot_new_to_clienteling, 
		d.month_tot_has1yrNonTerminatedSeller_ind,
		d.month_tot_allConnections_churned_ind, 
		d.month_tot_FP_churned_ind, 
		--d.month_tot_boughtFromConnection,
		--d.month_tot_boughtFromSB,
		d.month_tot_phoneMarketable_Y,
		d.month_tot_phoneMarketable_N,
		d.month_tot_emailMarketable_Y,
		d.month_tot_emailMarketable_N,
		
		d.month_tot_fls_trips, 
		d.month_tot_ncom_trips,
		d.month_tot_sb_ncom_trips, 
		
		d.month_tot_gross_fls_spend,  
		d.month_tot_gross_ncom_spend,  
		d.month_tot_sb_gross_ncom_spend, 
		
		--AGGREGATED TO STORE LEVEL 
		c.store_tot_cnt_OptedInToday, 
		c.store_tot_cnt_OptedOutToday, 
		c.store_tot_cnt_Consented_wSeller, 

		c.store_tot_hasOnlyTerminatedSeller_ind, 
		c.store_tot_new_to_clienteling, 
		c.store_tot_has1yrNonTerminatedSeller_ind,
		c.store_tot_allConnections_churned_ind, 
		c.store_tot_FP_churned_ind, 
		--c.store_tot_boughtFromConnection,
		--c.store_tot_boughtFromSB,
		c.store_tot_phoneMarketable_Y,
		c.store_tot_phoneMarketable_N,
		c.store_tot_emailMarketable_Y,
		c.store_tot_emailMarketable_N,
		
		c.store_tot_fls_trips, 
		c.store_tot_ncom_trips, 
		c.store_tot_sb_ncom_trips, 
		
		c.store_tot_fls_trips_Connection, 
		c.store_tot_ncom_trips_Connection, 
		c.store_tot_sb_ncom_trips_Connection, 
		
		c.store_tot_fls_trips_NonConnection, 
		c.store_tot_ncom_trips_NonConnection, 
		c.store_tot_sb_ncom_trips_NonConnection, 
		
		c.store_tot_fls_trips_woSeller, 
		c.store_tot_ncom_trips_woSeller, 
		c.store_tot_sb_ncom_trips_woSeller, 
		
		c.store_tot_gross_fls_spend, 
		c.store_tot_gross_ncom_spend, 
		c.store_tot_sb_gross_ncom_spend, 		--40
		

		--AGGREGATED TO SELLER LEVEL
		sum(a.optedInToday) as cnt_optedInToday,
		sum(a.optedOutToday) as cnt_optedOutToday,
		sum(a.consented_wSeller) as cnt_Consented_wSeller,			--maybe add more customer attributes
		sum(b.has1yrNonTerminatedSeller_ind) as cnt_has1yrNonTerminatedSeller,
		sum(b.hasOnlyTerminatedSeller_ind) as cnt_hasOnlyTerminatedSeller,
		sum(b.allConnections_churned_ind) as cnt_allConnections_churned,
		sum(b.fp_churned_ind) as cnt_fp_churned,
		sum(b.new_to_clienteling) as cnt_new_to_clienteling,		--ADD THIS TO STORE AND DATE AGGREGATION!!!
		sum(case when b.phone_marketability='Y' then 1 else 0 end) as cnt_phoneMarketable_Y,
		sum(case when b.phone_marketability='N' then 1 else 0 end) as cnt_phoneMarketable_N,
		sum(case when b.email_marketability='Y' then 1 else 0 end) as cnt_emailMarketable_Y,
		sum(case when b.email_marketability='N' then 1 else 0 end) as cnt_emailMarketable_N,
		
		sum(a.consented_1mo_wSeller) as cnt_1mo_wSeller,
		sum(a.consented_3mo_wSeller) as cnt_3mo_wSeller,
		sum(a.consented_6mo_wSeller) as cnt_6mo_wSeller,
		sum(a.consented_1yr_wSeller) as cnt_1yr_wSeller,
		sum(a.consented_2yr_wSeller) as cnt_2yr_wSeller,
		
		--to consented seller TOTAL SALES
		sum(a.seller_fls_trips) as toConsented_fls_trips,
		sum(a.seller_ncom_trips) as toConsented_ncom_trips,
		sum(a.seller_sb_ncom_trips) as toConsented_sb_ncom_trips,
		
		sum(a.seller_gross_fls_spend) as toConsented_gross_fls_spend,
		sum(a.seller_gross_ncom_spend) as toConsented_gross_ncom_spend,
		sum(a.seller_sb_gross_ncom_spend) as toConsented_sb_gross_ncom_spend

	from consented_trx_acp a --@fm, acp, seller level
																
	join customer_attrb b on  a.acp_id = b.acp_id			--@fm, acp level			
							and a.fm_range = b.fm_range	
	
	join store_attrb c on coalesce(a.payroll_store,'na') = coalesce(c.payroll_store,'na')		--@fm, store level
							and a.fm_range = c.fm_range	
							
	join month_attrb d on a.fm_range = d.fm_range		--@fm level
	--BREAK INTO PART 1
	where (a.fm_range like '%2023%' or a.fm_range like '%2025%')
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,
			37,38,39,	40,41,42,	43,44,45		,46,47,		48,49	,50,51,52,53		,54 --,55


) WITH DATA PRIMARY INDEX (fm_range, seller_id, payroll_store)
ON COMMIT PRESERVE ROWS;


--drop table outp_pt2;
CREATE  MULTISET VOLATILE TABLE OUTP_PT2 AS (
	sel a.fm_range, 
		a.fiscal_month, a.fiscal_quarter, a.fiscal_year, 
		
		--seller attributes
		coalesce(a.seller_id,'NA') as seller_id,
		--coalesce(a.seller_name, 'NA') as  seller_name,
		coalesce(a.worker_status, 'NA') as  worker_status,
		coalesce(a.payroll_store, 'NA') as  payroll_store,
		coalesce(a.payroll_store_description, 'NA') as  payroll_store_description,
		coalesce(a.payroll_department_description, 'NA') as  payroll_department_description,		--10 fields so far		2.14.24
	
		--AGGREGATED TO DATE LEVEL			--15 when cut down
		d.month_tot_cnt_OptedInToday, 
		d.month_tot_cnt_OptedOutToday, 
		d.month_tot_cnt_Consented_wSeller, 
		
		d.month_tot_hasOnlyTerminatedSeller_ind, 
		d.month_tot_new_to_clienteling, 
		d.month_tot_has1yrNonTerminatedSeller_ind,
		d.month_tot_allConnections_churned_ind, 
		d.month_tot_FP_churned_ind, 
		--d.month_tot_boughtFromConnection,
		--d.month_tot_boughtFromSB,
		d.month_tot_phoneMarketable_Y,
		d.month_tot_phoneMarketable_N,
		d.month_tot_emailMarketable_Y,
		d.month_tot_emailMarketable_N,
		
		d.month_tot_fls_trips, 
		d.month_tot_ncom_trips,
		d.month_tot_sb_ncom_trips, 
		
		d.month_tot_gross_fls_spend,  
		d.month_tot_gross_ncom_spend,  
		d.month_tot_sb_gross_ncom_spend, 
		
		--AGGREGATED TO STORE LEVEL 
		c.store_tot_cnt_OptedInToday, 
		c.store_tot_cnt_OptedOutToday, 
		c.store_tot_cnt_Consented_wSeller, 

		c.store_tot_hasOnlyTerminatedSeller_ind, 
		c.store_tot_new_to_clienteling, 
		c.store_tot_has1yrNonTerminatedSeller_ind,
		c.store_tot_allConnections_churned_ind, 
		c.store_tot_FP_churned_ind, 
		--c.store_tot_boughtFromConnection,
		--c.store_tot_boughtFromSB,
		c.store_tot_phoneMarketable_Y,
		c.store_tot_phoneMarketable_N,
		c.store_tot_emailMarketable_Y,
		c.store_tot_emailMarketable_N,
		
		c.store_tot_fls_trips, 
		c.store_tot_ncom_trips, 
		c.store_tot_sb_ncom_trips, 
		
		c.store_tot_fls_trips_Connection, 
		c.store_tot_ncom_trips_Connection, 
		c.store_tot_sb_ncom_trips_Connection, 
		
		c.store_tot_fls_trips_NonConnection, 
		c.store_tot_ncom_trips_NonConnection, 
		c.store_tot_sb_ncom_trips_NonConnection, 
		
		c.store_tot_fls_trips_woSeller, 
		c.store_tot_ncom_trips_woSeller, 
		c.store_tot_sb_ncom_trips_woSeller, 
		
		c.store_tot_gross_fls_spend, 
		c.store_tot_gross_ncom_spend, 
		c.store_tot_sb_gross_ncom_spend, 		--40
		

		--AGGREGATED TO SELLER LEVEL
		sum(a.optedInToday) as cnt_optedInToday,
		sum(a.optedOutToday) as cnt_optedOutToday,
		sum(a.consented_wSeller) as cnt_Consented_wSeller,			--maybe add more customer attributes
		sum(b.has1yrNonTerminatedSeller_ind) as cnt_has1yrNonTerminatedSeller,
		sum(b.hasOnlyTerminatedSeller_ind) as cnt_hasOnlyTerminatedSeller,
		sum(b.allConnections_churned_ind) as cnt_allConnections_churned,
		sum(b.fp_churned_ind) as cnt_fp_churned,
		sum(b.new_to_clienteling) as cnt_new_to_clienteling,		--ADD THIS TO STORE AND DATE AGGREGATION!!!
		sum(case when b.phone_marketability='Y' then 1 else 0 end) as cnt_phoneMarketable_Y,
		sum(case when b.phone_marketability='N' then 1 else 0 end) as cnt_phoneMarketable_N,
		sum(case when b.email_marketability='Y' then 1 else 0 end) as cnt_emailMarketable_Y,
		sum(case when b.email_marketability='N' then 1 else 0 end) as cnt_emailMarketable_N,
		
		sum(a.consented_1mo_wSeller) as cnt_1mo_wSeller,
		sum(a.consented_3mo_wSeller) as cnt_3mo_wSeller,
		sum(a.consented_6mo_wSeller) as cnt_6mo_wSeller,
		sum(a.consented_1yr_wSeller) as cnt_1yr_wSeller,
		sum(a.consented_2yr_wSeller) as cnt_2yr_wSeller,
		
		--to consented seller TOTAL SALES
		sum(a.seller_fls_trips) as toConsented_fls_trips,
		sum(a.seller_ncom_trips) as toConsented_ncom_trips,
		sum(a.seller_sb_ncom_trips) as toConsented_sb_ncom_trips,
		
		sum(a.seller_gross_fls_spend) as toConsented_gross_fls_spend,
		sum(a.seller_gross_ncom_spend) as toConsented_gross_ncom_spend,
		sum(a.seller_sb_gross_ncom_spend) as toConsented_sb_gross_ncom_spend

	from consented_trx_acp a --@fm, acp, seller level
																
	join customer_attrb b on  a.acp_id = b.acp_id			--@fm, acp level			
							and a.fm_range = b.fm_range	
	
	join store_attrb c on coalesce(a.payroll_store,'na') = coalesce(c.payroll_store,'na')		--@fm, store level
							and a.fm_range = c.fm_range	
							
	join month_attrb d on a.fm_range = d.fm_range		--@fm level
	--BREAK INTO PART 2
	where not (a.fm_range like '%2023%' or a.fm_range like '%2025%')
	
	
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,
			37,38,39,	40,41,42,	43,44,45		,46,47,		48,49	,50,51,52,53		,54  --,55


) WITH DATA PRIMARY INDEX (fm_range, seller_id, payroll_store)
ON COMMIT PRESERVE ROWS;


DELETE FROM {cli_t2_schema}.consented_customer_seller_monthly
		WHERE fm_range in (
			select distinct fm_range from outp_pt1
			union
			select distinct fm_range from outp_pt2
			)
;


INSERT INTO {cli_t2_schema}.consented_customer_seller_monthly
    	SELECT DISTINCT 
			A.*,
	        CURRENT_TIMESTAMP AS dw_sys_load_tmstp
		FROM (
			select * FROM outp_pt1
	        UNION 
    	    select * FROM outp_pt2
		) a
;

    
COLLECT STATISTICS COLUMN (fm_range, seller_id, payroll_store) ON {cli_t2_schema}.consented_customer_seller_monthly; 


grant select on {cli_t2_schema}.consented_customer_seller_monthly to public;


SET QUERY_BAND = NONE FOR SESSION;
