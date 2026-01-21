--Delete yesterdays data	
DELETE FROM t2dl_das_open_to_buy.AB_CM_ORDERS_CURRENT;

--Insert current records 
INSERT INTO t2dl_das_open_to_buy.AB_CM_ORDERS_CURRENT
	SELECT 
		OREPLACE(CHANNEL,'"','') AS CHANNEL
		,NRF_COLOR_CODE
		,STYLE_ID
		,OREPLACE(STYLE_GROUP_ID,'"','') AS STYLE_GROUP_ID
		,OREPLACE(CLASS_NAME,'"','') AS CLASS_NAME
		,OREPLACE(SUBCLASS_NAME,'"','') AS SUBCLASS_NAME
		,OREPLACE(NPG_IND,'"','') AS NPG_IND
		,DEPT_ID AS DEPT_ID
		,PLAN_SEASON_ID
		,OREPLACE(PLAN_KEY,'"','') AS PLAN_KEY
		,OREPLACE(SUPP_ID,'"','') AS SUPP_ID
		,OREPLACE(SUPP_NAME,'"','') AS SUPP_NAME
		,FISCAL_MONTH_ID
		,OREPLACE(OTB_EOW_DATE,'"','') AS OTB_EOW_DATE
		,OREPLACE(ORG_ID,'"','') AS ORG_ID
		,OREPLACE(VPN,'"','') AS VPN
		,OREPLACE(VPN_DESC,'"','') AS VPN_DESC
		,CLASS_ID
		,SUBCLASS_ID
		,OREPLACE(SUPP_COLOR,'"','') AS SUPP_COLOR
		,OREPLACE(IMPORT_COUNTRY_CODE,'"','') AS IMPORT_COUNTRY_CODE
		,OREPLACE(BANNER,'"','') AS BANNER
		,OREPLACE(PLAN_TYPE,'"','') AS PLAN_TYPE
		,OREPLACE(PO_KEY,'"','') AS PO_KEY
		,RCPT_UNITS
		,UNIT_COST_US
		,UNIT_COST_CA
		,SPCL_UNIT_COST_US
		,SPCL_UNIT_COST_CA
		,TTL_COST_US
		,TTL_COST_CA
		,UNIT_RTL_US
		,UNIT_RTL_CA
		,SPCL_UNIT_RTL_US
		,SPCL_UNIT_RTL_CA
		,OREPLACE(RCPT_RTL_TYPE,'"','')AS RCPT_RTL_TYPE
		,TTL_RTL_US
		,TTL_RTL_CA
		,OREPLACE(PLAN_COMMIT_IND,'"','') AS PLAN_COMMIT_IND
		,OREPLACE(CM_DOLLARS,'"','') AS CM_DOLLARS
		,PO_NUMBER
		,PLAN_SEASON_DESC
		,OREPLACE(LAST_COMMITTED_OTB_EOW_DATE,'"','') AS LAST_COMMITTED_OTB_EOW_DATE
		,CURRENT_DATE AS PROCESS_DT
	FROM t2dl_das_open_to_buy.AB_CM_ORDERS_STG;

COLLECT STATS
	PRIMARY INDEX(nrf_color_code,vpn,supp_id)
	,COLUMN(nrf_color_code)
	,COLUMN(vpn)
	,COLUMN(supp_id)
	,COLUMN(DEPT_ID)
	,COLUMN(CLASS_ID)
	,COLUMN(SUBCLASS_ID)
		ON t2dl_das_open_to_buy.ab_cm_orders_current;
	

--Insert daily records into historical table
DELETE FROM t2dl_das_open_to_buy.AB_CM_ORDERS_HISTORICAL
WHERE PROCESS_DT NOT IN (
							SELECT DISTINCT 
                                d.week_end_day_date 
                            FROM t2dl_das_open_to_buy.AB_CM_ORDERS_HISTORICAL  h 
                            JOIN prd_nap_usr_vws.day_cal_454_dim d
                              ON h.PROCESS_DT = d.day_date
                          )
;

INSERT INTO t2dl_das_open_to_buy.AB_CM_ORDERS_HISTORICAL 
	SELECT 
	  channel
      ,nrf_color_code
      ,style_id
      ,style_group_id
      ,class_name
      ,subclass_name
      ,npg_ind
      ,dept_id
      ,plan_season_id
      ,plan_key
      ,supp_id
      ,supp_name
      ,fiscal_month_id
      ,LAST_COMMITTED_OTB_EOW_DATE as otb_eow_date
      ,org_id
      ,vpn
      ,vpn_desc
      ,class_id
      ,subclass_id
      ,supp_color
      ,import_country_code
      ,banner
      ,plan_type
      ,po_key
      ,rcpt_units
      ,unit_cost_us
      ,unit_cost_ca
      ,spcl_unit_cost_us
      ,spcl_unit_cost_ca
      ,ttl_cost_us
      ,ttl_cost_ca
      ,unit_rtl_us
      ,unit_rtl_ca
      ,spcl_unit_rtl_us
      ,spcl_unit_rtl_ca
      ,rcpt_rtl_type
      ,ttl_rtl_us
      ,ttl_rtl_ca
      ,plan_commit_ind
      ,cm_dollars 
      ,process_dt 
	FROM t2dl_das_open_to_buy.AB_CM_ORDERS_CURRENT;

COLLECT STATS
	PRIMARY INDEX(nrf_color_code,vpn,supp_id)
	,COLUMN(nrf_color_code)
	,COLUMN(vpn)
	,COLUMN(supp_id)
	,COLUMN(DEPT_ID)
	,COLUMN(CLASS_ID)
	,COLUMN(SUBCLASS_ID)
		ON t2dl_das_open_to_buy.AB_CM_ORDERS_HISTORICAL;

--Drop STG table
DROP TABLE t2dl_das_open_to_buy.AB_CM_ORDERS_STG;