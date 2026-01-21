SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=twist_11521_ACE_ENG;
     Task_Name=twist_daily;'
     FOR SESSION VOLATILE;


ALTER TABLE T2DL_DAS_TWIST.twist_daily RENAME  loc_instock_ind TO mc_instock_ind;
ALTER TABLE T2DL_DAS_TWIST.twist_daily RENAME  market_instock_ind TO fc_instock_ind;


ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  ncom_instock_mkt_traffic TO ncom_fc_instock_traffic;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  fls_instock_mkt_traffic TO fls_fc_instock_traffic;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  rack_instock_mkt_traffic TO rack_fc_instock_traffic;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  rcom_instock_mkt_traffic TO rcom_fc_instock_traffic;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  fls_eoh TO fls_asoh;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  ncom_eoh TO ncom_asoh;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  rack_eoh TO rack_asoh;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  rcom_eoh TO rcom_asoh;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_489 TO asoh_489;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_568 TO asoh_568;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_584 TO asoh_584;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_599 TO asoh_599;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_659 TO asoh_659;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_873 TO asoh_5629;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  eoh_881 TO asoh_881;
ALTER TABLE T2DL_DAS_TWIST.twist_item_weekly RENAME  instock_873 TO instock_5629;



ALTER TABLE T2DL_DAS_TWIST.twist_summary_weekly RENAME  instock_mkt_traffic TO fc_instock_traffic;
ALTER TABLE T2DL_DAS_TWIST.twist_summary_weekly RENAME  instock_873 TO instock_5629;

SET QUERY_BAND = NONE FOR SESSION;
