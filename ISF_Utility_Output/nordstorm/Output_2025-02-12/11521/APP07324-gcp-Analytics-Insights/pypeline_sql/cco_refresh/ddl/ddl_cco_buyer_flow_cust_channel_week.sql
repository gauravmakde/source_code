SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=ddl_cco_buyer_flow_cust_channel_week_11521_ACE_ENG;
Task_Name=run_teradata_ddl_cco_buyer_flow_cust_channel_week;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************
 * CCO Buyer Flow Customer/Channel/Week-Level DDL file.
 * This file creates the production table T2DL_DAS_STRATEGY.cco_buyer_flow_cust_channel_week.
 ************************************************************************************/
/************************************************************************************/


--/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP(
     '{cco_t2_schema}',
     'cco_buyer_flow_cust_channel_week',
     OUT_RETURN_MSG);
--*/


CREATE MULTISET TABLE {cco_t2_schema}.cco_buyer_flow_cust_channel_week,--NO FALLBACK ,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    acp_id VARCHAR(50) CHARACTER SET UNICODE,
    week_idnt INTEGER,
    channel VARCHAR(20) CHARACTER SET UNICODE,
    buyer_flow VARCHAR(27) CHARACTER SET UNICODE COMPRESS (
        '1) New-to-JWN',
        '2) New-to-Channel (not JWN)',
        '3) Retained-to-Channel',
        '4) Reactivated-to-Channel'),
    AARE_acquired INTEGER COMPRESS (0,1),
    AARE_activated INTEGER COMPRESS (0,1),
    AARE_retained INTEGER COMPRESS (0,1),
    AARE_engaged INTEGER COMPRESS (0,1),
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX (acp_id, week_idnt, channel)
PARTITION BY RANGE_N(week_idnt BETWEEN 201601 AND 201652 EACH 1,
                                       201701 AND 201753 EACH 1,
                                       201801 AND 201852 EACH 1,
                                       201901 AND 201952 EACH 1,
                                       202001 AND 202052 EACH 1,
                                       202101 AND 202153 EACH 1,
                                       202201 AND 202253 EACH 1,
                                       202301 AND 202353 EACH 1,
                                       202401 AND 202453 EACH 1,
                                       202501 AND 202553 EACH 1,
                                       202601 AND 202653 EACH 1,
                                       202701 AND 202753 EACH 1,
                                       202801 AND 202853 EACH 1,
                                       202901 AND 202953 EACH 1,
                                       203001 AND 203053 EACH 1);


COLLECT STATISTICS COLUMN (acp_id) ON {cco_t2_schema}.cco_buyer_flow_cust_channel_week;
COLLECT STATISTICS COLUMN (week_idnt) ON {cco_t2_schema}.cco_buyer_flow_cust_channel_week;
COLLECT STATISTICS COLUMN (channel) ON {cco_t2_schema}.cco_buyer_flow_cust_channel_week;
COLLECT STATISTICS COLUMN (buyer_flow) ON {cco_t2_schema}.cco_buyer_flow_cust_channel_week;
COLLECT STATISTICS COLUMN (acp_id, week_idnt, channel) ON {cco_t2_schema}.cco_buyer_flow_cust_channel_week;


SET QUERY_BAND = NONE FOR SESSION;
