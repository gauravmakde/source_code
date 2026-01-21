SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_selling_permissions_11521_ACE_ENG;
     Task_Name=remote_selling_permissions;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6, 2024
Purpose: Grant Select access on a case by case basis to tables within Remote Sell T2 
*/
GRANT SELECT ON {remote_selling_t2_schema}.REMOTE_SELL_TRANSACTIONS TO 
T2DL_NAP_SVE_BATCH --Rebecca Liao (Ni's team)
, T2DL_NAP_SAR_BATCH --Sales and Returns
, T2DL_NAP_CCO_BATCH
, T2DL_NAP_DEV_BATCH
, T2DL_NAP_CLI_BATCH --Clienteling
, T2DL_TBLNAPRMT --Remote Sell Tableau SA
, T2DL_TBLNAPCLI --Clienteling Tableau SA
, Y6IK                --Lance Christenson
, DBDT                --Julie Liu
, YPYB                --Ryan Graves
, C35E                --Tamara Tangen
, GV53                --Jeff Reifman
, AE5B, X07V, E44K, FEWI, VF4X, HI7L,G45T,SNUI,HYDN    --Customer Profile Explorer
;


GRANT SELECT ON {remote_selling_t2_schema}.remote_sell_forecast TO 
T2DL_NAP_SVE_BATCH --Rebecca Liao (Ni's team)
, T2DL_NAP_SAR_BATCH --Sales and Returns
, T2DL_NAP_CCO_BATCH
, T2DL_NAP_DEV_BATCH
, T2DL_NAP_CLI_BATCH --Clienteling
, T2DL_TBLNAPRMT --Remote Sell Tableau SA
, T2DL_TBLNAPCLI --Clienteling Tableau SA
, Y6IK                --Lance Christenson
, DBDT                --Julie Liu
, YPYB                --Ryan Graves
, C35E                --Tamara Tangen
, GV53                --Jeff Reifman
;

GRANT SELECT ON {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY TO 
T2DL_NAP_SVE_BATCH --Rebecca Liao (Ni's team)
, T2DL_NAP_SAR_BATCH --Sales and Returns
, T2DL_NAP_CCO_BATCH
, T2DL_NAP_DEV_BATCH
, T2DL_NAP_CLI_BATCH --Clienteling
, T2DL_TBLNAPRMT --Remote Sell Tableau SA
, T2DL_TBLNAPCLI --Clienteling Tableau SA
, Y6IK                --Lance Christenson
, DBDT                --Julie Liu
, YPYB                --Ryan Graves
, C35E                --Tamara Tangen
, GV53                --Jeff Reifman
, NAPMKTGFP01  --Dmytro Velychko (Rakuten)
, AE5B, X07V, E44K, FEWI, VF4X, HI7L,G45T,SNUI,HYDN    --Customer Profile Explorer

;

SET QUERY_BAND = NONE FOR SESSION;
