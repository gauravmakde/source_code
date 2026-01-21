SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_jwn_dept_mmm_div_xref_fp_11521_ACE_ENG;
     Task_Name=ddl_jwn_dept_mmm_div_xref_fp;'
     FOR SESSION VOLATILE;

/*
Table definition for t2dl_das_mmm.jwn_dept_mmm_div_xref_fp

neustar mmm divisions don't directly map to jwn divisions
so this table maps to mmm divisions (fp) via dept_num

Team/Owner: AE
Date Created/Modified: 11/23/2022
*/

 create  multiset table {mmm_t2_schema}.jwn_dept_mmm_div_xref_fp
 (dept_num integer,
 mmm_division varchar(60) character set unicode not casespecific) 
 primary index (dept_num); 

-- Table Comment (STANDARD)
COMMENT ON  {mmm_t2_schema}.jwn_dept_mmm_div_xref_fp IS 'table maps to mmm divisions (fp) via dept_num';

SET QUERY_BAND = NONE FOR SESSION;