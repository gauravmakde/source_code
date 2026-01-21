SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=sales_and_returns_fact_11521_ACE_ENG;
     Task_Name=sales_and_returns_fact;'
     FOR SESSION VOLATILE;



/*Change T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT
to be a view (retain same name as original table since name exists
in significant number of existing queries).  Table name
changed to suffix with _base. this is to prevent concurrent
access blocking*/


REPLACE VIEW T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT AS
LOCK ROW FOR ACCESS
SELECT * FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT_BASE;


SET QUERY_BAND = NONE FOR SESSION;