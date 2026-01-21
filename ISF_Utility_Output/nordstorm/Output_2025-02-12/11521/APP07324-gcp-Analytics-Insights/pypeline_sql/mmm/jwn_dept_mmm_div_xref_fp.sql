SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=jwn_dept_mmm_div_xref_fp_11521_ACE_ENG;
     Task_Name=jwn_dept_mmm_div_xref_fp;'
     FOR SESSION VOLATILE;

  /*
original version mar 2022
Lance Christenson

refresh neustar jwn_dept_mmm_div_xref_fp
providing xref from jwn dept to mmm div 

Team/Owner: AE
Date Created/Modified: 11/23/2022
*/

CREATE MULTISET VOLATILE TABLE temp_mmm_div_xref_fp AS (
select 
dept_num, 
upper(case 
when subdivision_name like '%last chance%' then 'other/non-merch'
when subdivision_name like '%w%m%n%spec%' then 'womens specialized'
when subdivision_name like '%beauty%' then 'beauty'
when subdivision_name like '%fragrance%' then 'beauty'
when subdivision_name like '%cosmetics%' then 'beauty'
when subdivision_name like '%w%m%n%shoe%' then 'womens shoes'
when subdivision_name like '%w%m%n%apparel%' then 'womens apparel'
when subdivision_name like '%men%shoe%' then 'mens shoes'
when subdivision_name like '%kid%shoe%' then 'kids shoes'
when subdivision_name like '%design%' then 'designer'
when subdivision_name like '%kid%wear%' then 'kids wear'
when subdivision_name like '%kid%app%' then 'kids wear'
when subdivision_name like '%boy%' then 'kids wear'
when subdivision_name like '%girl%' then 'kids wear'
when subdivision_name like '%men%wear%' then 'mens wear'
when subdivision_name like '%yc%' then 'yc'
when subdivision_name like '%access%' then 'accessories/at home'
when subdivision_name like '%home%' then 'accessories/at home'
when subdivision_name like '%merch%proj%' then 'merch projects'
when subdivision_name like '%other%' then 'other/non-merch'
 --
when division_name like '%last chance%' then 'other/non-merch'
when division_name like '%w%m%n%spec%' then 'womens specialized'
when division_name like '%beauty%' then 'beauty'
when division_name like '%fragrance%' then 'beauty'
when division_name like '%cosmetics%' then 'beauty'
when division_name like '%w%m%n%shoe%' then 'womens shoes'
when division_name like '%w%m%n%apparel%' then 'womens apparel'
when division_name like '%men%shoe%' then 'mens shoes'
when division_name like '%kid%shoe%' then 'kids shoes'
when division_name like '%design%' then 'designer'
when division_name like '%kid%wear%' then 'kids wear'
when division_name like '%kid%app%' then 'kids wear'
when division_name like '%boy%' then 'kids wear'
when division_name like '%girl%' then 'kids wear'
when division_name like '%men%wear%' then 'mens wear'
when division_name like '%yc%' then 'yc'
when division_name like '%access%' then 'accessories/at home'
when division_name like '%home%' then 'accessories/at home'
when division_name like '%merch%proj%' then 'merch projects'
when division_name like '%other%' then 'other/non-merch'
 --
when dept_name like '%last chance%' then 'other/non-merch'
when dept_name like '%w%m%n%spec%' then 'womens specialized'
when dept_name like '%beauty%' then 'beauty'
when dept_name like '%fragrance%' then 'beauty'
when dept_name like '%cosmetics%' then 'beauty'
when dept_name like '%w%m%n%shoe%' then 'womens shoes'
when dept_name like '%w%m%n%apparel%' then 'womens apparel'
when dept_name like '%men%shoe%' then 'mens shoes'
when dept_name like '%kid%shoe%' then 'kids shoes'
when dept_name like '%design%' then 'designer'
when dept_name like '%kid%wear%' then 'kids wear'
when dept_name like '%kid%app%' then 'kids wear'
when dept_name like '%boy%' then 'kids wear'
when dept_name like '%girl%' then 'kids wear'
when dept_name like '%men%wear%' then 'mens wear'
when dept_name like '%yc%' then 'yc'
when dept_name like '%access%' then 'accessories/at home'
when dept_name like '%home%' then 'accessories/at home'
when dept_name like '%merch%proj%' then 'merch projects'
when dept_name like '%other%' then 'other/non-merch'
 --
when division_name like '%w%m%n%' then 'womens apparel'
when division_name like '%kid%' then 'kids wear'
when division_name like '%men%' then 'mens wear'
when subdivision_name like '%w%m%n%' then 'womens apparel'
when subdivision_name like '%kid%' then 'kids wear'
when subdivision_name like '%men%' then 'mens wear'
when dept_name like '%w%m%n%' then 'womens apparel'
when dept_name like '%kid%' then 'kids wear'
when dept_name like '%men%' then 'mens wear'
else 'other/non-merch' end) as mmm_division
 --
from prd_nap_usr_vws.department_dim  dpt
where dpt.dept_subtype_desc in ( 'merchandise owned', 'services owned')
) 
WITH DATA
PRIMARY INDEX(dept_num)
ON COMMIT PRESERVE ROWS
;




delete from {mmm_t2_schema}.jwn_dept_mmm_div_xref_fp;

insert into {mmm_t2_schema}.jwn_dept_mmm_div_xref_fp
select * from temp_mmm_div_xref_fp; 

collect statistics
column (dept_num)
on  
{mmm_t2_schema}.jwn_dept_mmm_div_xref_fp; 


SET QUERY_BAND = NONE FOR SESSION;