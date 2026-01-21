
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=ap_plan_act_fct/ap_plan_act_fct_wkly;
---Task_Name=t2_ap_plan_act_wrk_truncate;'*/


BEGIN
SET ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk;


--ET;
/*SET QUERY_BAND = NONE FOR SESSION;*/

EXCEPTION WHEN ERROR THEN
--ET;
RAISE USING MESSAGE = @@error.message;
END;
END;