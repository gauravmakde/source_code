/*
ast_pipeline_prod_main_size_curve_eval_adoption_query.sql
Author: Paria Avij
project: Size curve evaluation
Purpose: Create a table for size curve dashboard monthly adoption view
Date Created: 12/26/23
Datalab: t2dl_das_size,

*/

--drop table date_m;
CREATE MULTISET VOLATILE TABLE date_m AS(
SELECT 
	
	 MONTH_IDNT
	,MONTH_LABEL
	,QUARTER_IDNT 
	,QUARTER_LABEL 
	,fiscal_halfyear_num
	,FISCAL_YEAR_NUM 
	,half_label
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM  
group by 1,2,3,4,5,6,7)

WITH DATA
   PRIMARY INDEX(MONTH_IDNT)
   ON COMMIT PRESERVE ROWS;


--drop table adoption_1_m;
create multiset volatile table adoption_1_m as
 (select
 a.fiscal_month as fiscal_month_num,
 ca.month_label,
 ca.fiscal_halfyear_num as fiscal_halfyear_num,
 ca.half_label,
 banner,
 case when channel_id= 110 then 'FLS'
 	  when channel_id= 210 then 'RACK'
 	  when channel_id= 250 then 'NRHL'
 	  when channel_id= 120 then 'N.COM'
 	  else null end as chnl_idnt,
 dept_id,
 size_profile,
 sum(rcpt_dollars) as rcpt_dollars,
 sum(rcpt_units) as rcpt_units 
from t2dl_das_size.adoption_metrics a
join date_m ca
on a.fiscal_month = ca.month_idnt
where chnl_idnt is not null
group by 1,2,3,4,5,6,7,8
) with data  primary index (dept_id, chnl_idnt,fiscal_month_num ) on commit preserve rows;


--with revised adoption monthly level
--drop table adoption_percetange_m_1_J_rev;
create multiset volatile table adoption_percetange_m_1_J_rev
as(
select
    TRIM(fiscal_halfyear_num) as fiscal_halfyear_num,
    TRIM(half_label) as half_label,
	TRIM(fiscal_month_num) as fiscal_month_num,
    TRIM(month_label) as month_label,
	dept_id,
	chnl_idnt,
	TRIM(b.DIVISION_NUM||','||b.DIVISION_NAME) AS Division,
    TRIM(b.SUBDIVISION_NUM||','||b.SUBDIVISION_NAME) AS Subdivision,
    TRIM(a.DEPT_ID||','||b.DEPT_NAME) AS Department,	
	Coalesce(SUM(case when size_profile in ('ARTS', 'EXISTING CURVE', 'NONE', 'OTHER') then rcpt_units end),0) as Non_sc_rcpt_units,
	Coalesce(SUM(case when size_profile='ARTS' then rcpt_units end),0) as ARTS_rcpt_units,
	Coalesce(SUM(case when size_profile='EXISTING CURVE' then rcpt_units end),0) as existing_curves_rcpt_units,
	Coalesce(SUM(case when size_profile='NONE' then rcpt_units end),0) as none_rcpt_units,
	Coalesce(SUM(case when size_profile='OTHER' then rcpt_units end),0) as other_rcpt_units,
	Coalesce(SUM(case when size_profile in ('DSA') and chnl_idnt='FLS'  then Coalesce(rcpt_units, 0) end),0) as Sc_FLS_rcpt_units,
	Coalesce(SUM(case when size_profile in ('DSA') and chnl_idnt='RACK' then Coalesce(rcpt_units, 0) end),0) as Sc_RACK_rcpt_units,
	Coalesce(SUM(case when size_profile in ('DSA') and chnl_idnt='N.COM' then Coalesce(rcpt_units, 0) end),0) as Sc_NCOM_rcpt_units,
	Coalesce(SUM(case when size_profile in ('DSA') and chnl_idnt='NRHL' then Coalesce(rcpt_units, 0) end),0) as Sc_NRHL_rcpt_units,
	(Sc_FLS_rcpt_units + Sc_RACK_rcpt_units + Non_sc_rcpt_units + Sc_NCOM_rcpt_units + Sc_NRHL_rcpt_units) as Ttl_rcpt_units,
	(Sc_FLS_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_Sc_FLS_rcpt_units,
	(Sc_RACK_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_Sc_RACK_rcpt_units,
	(Sc_NCOM_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_Sc_NCOM_rcpt_units,
	(Sc_NRHL_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_Sc_NRHL_rcpt_units,
	(case when Percent_Sc_FLS_rcpt_units>= 70 or Percent_Sc_RACK_rcpt_units >= 70 or Percent_Sc_NCOM_rcpt_units >= 70 or Percent_Sc_NRHL_rcpt_units >= 70 then 'High Adopting'
      when Percent_Sc_FLS_rcpt_units>=30 and Percent_Sc_FLS_rcpt_units<70 or Percent_Sc_RACK_rcpt_units >=30 and Percent_Sc_RACK_rcpt_units <70 or Percent_Sc_NCOM_rcpt_units >= 30 and Percent_Sc_NCOM_rcpt_units <70 or Percent_Sc_NRHL_rcpt_units >= 30 and Percent_Sc_NRHL_rcpt_units < 70 then 'Medium Adopting' 
      when Percent_Sc_FLS_rcpt_units< 30 or Percent_Sc_RACK_rcpt_units < 30 or Percent_Sc_NCOM_rcpt_units < 30 or Percent_Sc_NRHL_rcpt_units < 30 then 'Low Adopting' end) as adoption,
    (ARTS_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_arts_rcpt_units,
    (existing_curves_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_existing_curves_rcpt_units,
    (none_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_none_rcpt_units,
    (other_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) as Percent_other_rcpt_units,
    (Percent_none_rcpt_units+Percent_other_rcpt_units+Percent_existing_curves_rcpt_units+ Percent_arts_rcpt_units+Percent_Sc_RACK_rcpt_units+Percent_Sc_FLS_rcpt_units+Percent_Sc_NCOM_rcpt_units+Percent_Sc_NRHL_rcpt_units) as total_percent
from
	adoption_1_m a
 left join prd_nap_usr_vws.department_dim b
 on a.dept_id=b.dept_num
group by 1,2,3,4,5,6,7,8,9)
with data primary index (dept_id,chnl_idnt,fiscal_month_num )on commit preserve rows;


DELETE FROM {environment_schema}.size_curve_evaluation_adoption_view{env_suffix};


INSERT INTO {environment_schema}.size_curve_evaluation_adoption_view{env_suffix}
SELECT a.*
       ,CURRENT_TIMESTAMP AS update_timestamp
FROM adoption_percetange_m_1_J_rev a




