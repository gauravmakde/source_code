SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=jwn_dept_mmm_div_xref_op_11521_ACE_ENG;
     Task_Name=jwn_dept_mmm_div_xref_op;'
     FOR SESSION VOLATILE;

/*
original version mar 2022
Lance Christenson

refresh neustar jwn_dept_mmm_div_xref_op
providing xref from jwn dept to mmm op div 

Team/Owner: AE
Date Created/Modified: 11/23/2022
*/

CREATE MULTISET VOLATILE TABLE temp_mmm_div_xref_op AS (
select
dept_num,  
upper(case 
 --
when subdivision_name like '%w%m%n%spec%' then 'op womens apparel'
when subdivision_name like '%beauty%' then 'op accessories/home/cosmetics'
when subdivision_name like '%fragrance%' then 'op accessories/home/cosmetics'
when subdivision_name like '%cosmetics%' then 'op accessories/home/cosmetics'
when subdivision_name like '%w%m%n%shoe%' then 'op shoes'
when subdivision_name like '%w%m%n%apparel%' then 'op womens apparel'
when subdivision_name like '%men%shoe%' then 'op shoes'
when subdivision_name like '%kid%shoe%' then 'op shoes'
when subdivision_name like '%kid%wear%' then 'op mens/kids'
when subdivision_name like '%kid%app%' then 'op mens/kids'
when subdivision_name like '%men%wear%' then 'op mens/kids'
when subdivision_name like '%yc%' then 'op coats/kids/specialized'
when subdivision_name like '%access%' then 'op accessories/home/cosmetics'
when subdivision_name like '%home%' then 'op accessories/home/cosmetics'
when subdivision_name like '%merch%proj%' then 'merch projects'
 --
 when division_name like '%w%m%n%spec%' then 'op womens apparel'
when division_name like '%beauty%' then 'op accessories/home/cosmetics'
when division_name like '%fragrance%' then 'op accessories/home/cosmetics'
when division_name like '%cosmetics%' then 'op accessories/home/cosmetics'
when division_name like '%w%m%n%shoe%' then 'op shoes'
when division_name like '%w%m%n%apparel%' then 'op womens apparel'
when division_name like '%men%shoe%' then 'op shoes'
when division_name like '%kid%shoe%' then 'op shoes'
when division_name like '%kid%wear%' then 'op mens/kids'
when division_name like '%kid%app%' then 'op mens/kids'
when division_name like '%men%wear%' then 'op mens/kids'
when division_name like '%yc%' then 'op coats/kids/specialized'
when division_name like '%access%' then 'op accessories/home/cosmetics'
when division_name like '%home%' then 'op accessories/home/cosmetics'
when division_name like '%merch%proj%' then 'merch projects'
 --
when dept_name like '%w%m%n%spec%' then 'op womens apparel'
when dept_name like '%beauty%' then 'op accessories/home/cosmetics'
when dept_name like '%fragrance%' then 'op accessories/home/cosmetics'
when dept_name like '%cosmetics%' then 'op accessories/home/cosmetics'
when dept_name like '%w%m%n%shoe%' then 'op shoes'
when dept_name like '%w%m%n%apparel%' then 'op womens apparel'
when dept_name like '%men%shoe%' then 'op shoes'
when dept_name like '%kid%shoe%' then 'op shoes'
when dept_name like '%kid%wear%' then 'op mens/kids'
when dept_name like '%kid%app%' then 'op mens/kids'
when dept_name like '%men%wear%' then 'op mens/kids'
when dept_name like '%yc%' then 'op coats/kids/specialized'
when dept_name like '%access%' then 'op accessories/home/cosmetics'
when dept_name like '%home%' then 'op accessories/home/cosmetics'
when dept_name like '%merch%proj%' then 'merch projects'
 --
when division_name like '%w%m%n%' then 'op womens apparel'
when division_name like '%kid%' then 'op mens/kids'
when division_name like '%boy%' then 'op mens/kids'
when division_name like '%girl%' then 'op mens/kids'
when division_name like '%men%' then 'op mens/kids'
when subdivision_name like '%w%m%n%' then 'op womens apparel'
when subdivision_name like '%kid%' then 'op mens/kids'
when subdivision_name like '%boy%' then 'op mens/kids'
when subdivision_name like '%girl%' then 'op mens/kids'
when subdivision_name like '%men%' then 'op mens/kids'
when dept_name like '%w%m%n%' then 'op womens apparel'
when dept_name like '%kid%' then 'op mens/kids'
when dept_name like '%boy%' then 'op mens/kids'
when dept_name like '%girl%' then 'op mens/kids'
when dept_name like '%men%' then 'op mens/kids'
when division_name like '%design%' then 'op womens apparel'
else 'op womens apparel' end) as mmm_division
 --
from prd_nap_usr_vws.department_dim  dpt
where dpt.dept_subtype_desc in ( 'merchandise owned', 'services owned')
) 
WITH DATA
PRIMARY INDEX(dept_num)
ON COMMIT PRESERVE ROWS
;


delete from {mmm_t2_schema}.jwn_dept_mmm_div_xref_op;

insert into {mmm_t2_schema}.jwn_dept_mmm_div_xref_op
select * from temp_mmm_div_xref_op;  
;

collect statistics
column (dept_num)
on  
{mmm_t2_schema}.jwn_dept_mmm_div_xref_op; 


SET QUERY_BAND = NONE FOR SESSION;