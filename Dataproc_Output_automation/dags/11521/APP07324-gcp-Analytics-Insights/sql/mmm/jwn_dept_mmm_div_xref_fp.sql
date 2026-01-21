BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=jwn_dept_mmm_div_xref_fp_11521_ACE_ENG;
---     Task_Name=jwn_dept_mmm_div_xref_fp;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS temp_mmm_div_xref_fp AS
SELECT dept_num,
 UPPER(CASE
   WHEN LOWER(subdivision_name) LIKE LOWER('%last chance%')
   THEN 'other/non-merch'
   WHEN LOWER(subdivision_name) LIKE LOWER('%w%m%n%spec%')
   THEN 'womens specialized'
   WHEN LOWER(subdivision_name) LIKE LOWER('%beauty%')
   THEN 'beauty'
   WHEN LOWER(subdivision_name) LIKE LOWER('%fragrance%')
   THEN 'beauty'
   WHEN LOWER(subdivision_name) LIKE LOWER('%cosmetics%')
   THEN 'beauty'
   WHEN LOWER(subdivision_name) LIKE LOWER('%w%m%n%shoe%')
   THEN 'womens shoes'
   WHEN LOWER(subdivision_name) LIKE LOWER('%w%m%n%apparel%')
   THEN 'womens apparel'
   WHEN LOWER(subdivision_name) LIKE LOWER('%men%shoe%')
   THEN 'mens shoes'
   WHEN LOWER(subdivision_name) LIKE LOWER('%kid%shoe%')
   THEN 'kids shoes'
   WHEN LOWER(subdivision_name) LIKE LOWER('%design%')
   THEN 'designer'
   WHEN LOWER(subdivision_name) LIKE LOWER('%kid%wear%')
   THEN 'kids wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%kid%app%')
   THEN 'kids wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%boy%')
   THEN 'kids wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%girl%')
   THEN 'kids wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%men%wear%')
   THEN 'mens wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%yc%')
   THEN 'yc'
   WHEN LOWER(subdivision_name) LIKE LOWER('%access%')
   THEN 'accessories/at home'
   WHEN LOWER(subdivision_name) LIKE LOWER('%home%')
   THEN 'accessories/at home'
   WHEN LOWER(subdivision_name) LIKE LOWER('%merch%proj%')
   THEN 'merch projects'
   WHEN LOWER(subdivision_name) LIKE LOWER('%other%')
   THEN 'other/non-merch'
   WHEN LOWER(division_name) LIKE LOWER('%last chance%')
   THEN 'other/non-merch'
   WHEN LOWER(division_name) LIKE LOWER('%w%m%n%spec%')
   THEN 'womens specialized'
   WHEN LOWER(division_name) LIKE LOWER('%beauty%')
   THEN 'beauty'
   WHEN LOWER(division_name) LIKE LOWER('%fragrance%')
   THEN 'beauty'
   WHEN LOWER(division_name) LIKE LOWER('%cosmetics%')
   THEN 'beauty'
   WHEN LOWER(division_name) LIKE LOWER('%w%m%n%shoe%')
   THEN 'womens shoes'
   WHEN LOWER(division_name) LIKE LOWER('%w%m%n%apparel%')
   THEN 'womens apparel'
   WHEN LOWER(division_name) LIKE LOWER('%men%shoe%')
   THEN 'mens shoes'
   WHEN LOWER(division_name) LIKE LOWER('%kid%shoe%')
   THEN 'kids shoes'
   WHEN LOWER(division_name) LIKE LOWER('%design%')
   THEN 'designer'
   WHEN LOWER(division_name) LIKE LOWER('%kid%wear%')
   THEN 'kids wear'
   WHEN LOWER(division_name) LIKE LOWER('%kid%app%')
   THEN 'kids wear'
   WHEN LOWER(division_name) LIKE LOWER('%boy%')
   THEN 'kids wear'
   WHEN LOWER(division_name) LIKE LOWER('%girl%')
   THEN 'kids wear'
   WHEN LOWER(division_name) LIKE LOWER('%men%wear%')
   THEN 'mens wear'
   WHEN LOWER(division_name) LIKE LOWER('%yc%')
   THEN 'yc'
   WHEN LOWER(division_name) LIKE LOWER('%access%')
   THEN 'accessories/at home'
   WHEN LOWER(division_name) LIKE LOWER('%home%')
   THEN 'accessories/at home'
   WHEN LOWER(division_name) LIKE LOWER('%merch%proj%')
   THEN 'merch projects'
   WHEN LOWER(division_name) LIKE LOWER('%other%')
   THEN 'other/non-merch'
   WHEN LOWER(dept_name) LIKE LOWER('%last chance%')
   THEN 'other/non-merch'
   WHEN LOWER(dept_name) LIKE LOWER('%w%m%n%spec%')
   THEN 'womens specialized'
   WHEN LOWER(dept_name) LIKE LOWER('%beauty%')
   THEN 'beauty'
   WHEN LOWER(dept_name) LIKE LOWER('%fragrance%')
   THEN 'beauty'
   WHEN LOWER(dept_name) LIKE LOWER('%cosmetics%')
   THEN 'beauty'
   WHEN LOWER(dept_name) LIKE LOWER('%w%m%n%shoe%')
   THEN 'womens shoes'
   WHEN LOWER(dept_name) LIKE LOWER('%w%m%n%apparel%')
   THEN 'womens apparel'
   WHEN LOWER(dept_name) LIKE LOWER('%men%shoe%')
   THEN 'mens shoes'
   WHEN LOWER(dept_name) LIKE LOWER('%kid%shoe%')
   THEN 'kids shoes'
   WHEN LOWER(dept_name) LIKE LOWER('%design%')
   THEN 'designer'
   WHEN LOWER(dept_name) LIKE LOWER('%kid%wear%')
   THEN 'kids wear'
   WHEN LOWER(dept_name) LIKE LOWER('%kid%app%')
   THEN 'kids wear'
   WHEN LOWER(dept_name) LIKE LOWER('%boy%')
   THEN 'kids wear'
   WHEN LOWER(dept_name) LIKE LOWER('%girl%')
   THEN 'kids wear'
   WHEN LOWER(dept_name) LIKE LOWER('%men%wear%')
   THEN 'mens wear'
   WHEN LOWER(dept_name) LIKE LOWER('%yc%')
   THEN 'yc'
   WHEN LOWER(dept_name) LIKE LOWER('%access%')
   THEN 'accessories/at home'
   WHEN LOWER(dept_name) LIKE LOWER('%home%')
   THEN 'accessories/at home'
   WHEN LOWER(dept_name) LIKE LOWER('%merch%proj%')
   THEN 'merch projects'
   WHEN LOWER(dept_name) LIKE LOWER('%other%')
   THEN 'other/non-merch'
   WHEN LOWER(division_name) LIKE LOWER('%w%m%n%')
   THEN 'womens apparel'
   WHEN LOWER(division_name) LIKE LOWER('%kid%')
   THEN 'kids wear'
   WHEN LOWER(division_name) LIKE LOWER('%men%')
   THEN 'mens wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%w%m%n%')
   THEN 'womens apparel'
   WHEN LOWER(subdivision_name) LIKE LOWER('%kid%')
   THEN 'kids wear'
   WHEN LOWER(subdivision_name) LIKE LOWER('%men%')
   THEN 'mens wear'
   WHEN LOWER(dept_name) LIKE LOWER('%w%m%n%')
   THEN 'womens apparel'
   WHEN LOWER(dept_name) LIKE LOWER('%kid%')
   THEN 'kids wear'
   WHEN LOWER(dept_name) LIKE LOWER('%men%')
   THEN 'mens wear'
   ELSE 'other/non-merch'
   END) AS mmm_division
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt
WHERE LOWER(dept_subtype_desc) IN (LOWER('merchandise owned'), LOWER('services owned'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.mmm_t2_schema}}.jwn_dept_mmm_div_xref_fp;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.mmm_t2_schema}}.jwn_dept_mmm_div_xref_fp
(SELECT * FROM temp_mmm_div_xref_fp);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (dept_num) on t2dl_das_mmm.jwn_dept_mmm_div_xref_fp;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
