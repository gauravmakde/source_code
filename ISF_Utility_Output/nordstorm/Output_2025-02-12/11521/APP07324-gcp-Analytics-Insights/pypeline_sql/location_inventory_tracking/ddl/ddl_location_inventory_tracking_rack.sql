SET QUERY_BAND = 'App_ID=APP09117;
     DAG_ID="ddl_location_inventory_tracking_rack_11521_ACE_ENG";
     Task_Name=ddl_location_inventory_tracking_rack;'
     FOR SESSION VOLATILE;




-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'location_inventory_tracking_rack', OUT_RETURN_MSG);





CREATE TABLE {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack (
pkey varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,ab_key varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,rp_spend_key varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,month_idnt int
,month_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,month_label varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,fiscal_month_num int
,month_start_day_date TIMESTAMP WITH TIME ZONE
,month_end_day_date TIMESTAMP WITH TIME ZONE
,wks_in_month int
,division varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- "Division",
,subdivision varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- "Subdivision",
,dept_label varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC--"Department Label",
,class_label varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC--"Class Label",
,dept_idnt int --as department --"Department",
,class_idnt int --as  "class",--"Class",
,loc_idnt int --as "Location",
,current_month_flag int
,last_month_flag int
,bop_plan int--as "BOP Plan",
,rcpt_plan int --"Rcpt Plan",
,sales_plan int --"Sales Plan",
,uncapped_bop int --"Uncapped BOP"
,peer_group_type_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,climate varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,store_dma_code varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- as "DMA Code"
,dma_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC
,location varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC--AS "Location"
,store_type_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC --AS "Store Type Desc" 
,gross_square_footage int-- AS "Gross Square Footage"
,store_open_date TIMESTAMP WITH TIME ZONE-- AS "Store Open Date" 
,store_close_date TIMESTAMP WITH TIME ZONE-- AS "Store Close Date"
,region_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Region Desc" 
,region_medium_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Region Medium Desc"
,region_short_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Region Short Desc" 
,business_unit_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC --AS "Business Unit Desc"
,group_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Group Desc"
,subgroup_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Subgroup Desc" 
,subgroup_medium_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Subgrou Med Desc"
,subgroup_short_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Subgroup Short Desc" 
,store_address_line_1 varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Address Line 1"
,store_address_city varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Address City"
,store_address_state varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Address State" 
,store_address_state_name varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Address State Name"
,store_postal_code varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Postal Code" 
,store_address_county varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Address County"
,store_country_code varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Country Code"
,store_country_name varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Store Country Name"
,store_location_latitude decimal(8,4)-- AS "Latitude"
,store_location_longitude decimal(8,4) -- AS "Longitude"  
,distribution_center_num int-- AS "Distribution Center Num" 
,distribution_center_name varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Distribution Center Name"
,channel_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Channel Desc"
,comp_status_desc varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC-- AS "Comp Status Desc"
,new_loc_flag int
,ab_oo_rcpt_u int
,total_bop_u_ty int
,rp_bop_u_ty int
,nrp_bop_u_ty int
,clearance_bop_u_ty int
,rp_clearance_bop_u_ty int
,nrp_clearance_bop_u_ty int
,total_bop_u_ly int
,rp_bop_u_ly int
,nrp_bop_u_ly int
,clearance_bop_u_ly int
,rp_clearance_bop_u_ly int
,nrp_clearance_bop_u_ly int
,total_ty_EOH_U int
,total_ty_ClearEOH_U int
,rp_ty_EOH_U int
,rp_ty_ClearEOH_U int
,nrp_ty_EOH_U int
,nrp_ty_ClearEOH_U int
,total_ly_EOH_U int
,total_ly_ClearEOH_U int
,rp_ly_EOH_U int
,rp_ly_ClearEOH_U int
,nrp_ly_EOH_U int
,nrp_ly_ClearEOH_U int
,total_transfer_in_pack_and_hold_units_ty int
,total_transfer_in_reserve_stock_units_ty int
,total_transfer_in_racking_units_ty int
,total_transfer_in_return_to_rack_units_ty int
,total_receipts_total_units_ty int
,total_jwn_gross_sales_total_units_ty int
,total_jwn_demand_total_units_ty int
,total_jwn_operational_gmv_total_units_ty int
,total_jwn_returns_total_units_ty int
,rp_transfer_in_pack_and_hold_units_ty int
,rp_transfer_in_reserve_stock_units_ty int
,rp_transfer_in_racking_units_ty int
,rp_transfer_in_return_to_rack_units_ty int
,rp_receipts_total_units_ty int
,rp_jwn_gross_sales_total_units_ty int
,rp_jwn_demand_total_units_ty int
,rp_jwn_operational_gmv_total_units_ty int
,rp_jwn_returns_total_units_ty int
,nrp_transfer_in_pack_and_hold_units_ty int
,nrp_transfer_in_reserve_stock_units_ty int
,nrp_transfer_in_racking_units_ty int
,nrp_transfer_in_return_to_rack_units_ty int
,nrp_receipts_total_units_ty int
,nrp_jwn_gross_sales_total_units_ty int
,nrp_jwn_demand_total_units_ty int
,nrp_jwn_operational_gmv_total_units_ty int
,nrp_jwn_returns_total_units_ty int
,total_transfer_in_pack_and_hold_units_ly int
,total_transfer_in_reserve_stock_units_ly int
,total_transfer_in_racking_units_ly int
,total_transfer_in_return_to_rack_units_ly int
,total_receipts_total_units_ly int
,total_jwn_gross_sales_total_units_ly int
,total_jwn_demand_total_units_ly int
,total_jwn_operational_gmv_total_units_ly int
,total_jwn_returns_total_units_ly int
,rp_transfer_in_pack_and_hold_units_ly int
,rp_transfer_in_reserve_stock_units_ly int
,rp_transfer_in_racking_units_ly int
,rp_transfer_in_return_to_rack_units_ly int
,rp_receipts_total_units_ly int
,rp_jwn_gross_sales_total_units_ly int
,rp_jwn_demand_total_units_ly int
,rp_jwn_operational_gmv_total_units_ly int
,rp_jwn_returns_total_units_ly int
,nrp_transfer_in_pack_and_hold_units_ly int
,nrp_transfer_in_reserve_stock_units_ly int
,nrp_transfer_in_racking_units_ly int
,nrp_transfer_in_return_to_rack_units_ly int
,nrp_receipts_total_units_ly int
,nrp_jwn_gross_sales_total_units_ly int
,nrp_jwn_demand_total_units_ly int
,nrp_jwn_operational_gmv_total_units_ly int
,nrp_jwn_returns_total_units_ly int
,nrp_po_oo_u int
,rp_po_oo_u int
,total_po_oo_u int
,rp_anticipated_rcpt_u_splits  decimal (12,2)
,rp_sales_forecast decimal (12,2)
,total_mth_forecast  decimal (12,2)
,prcnt_of_rp_forecast decimal (12,2)
,in_transit_qty decimal (12,2)
,rp_in_transit_u decimal (12,2)
,nrp_in_transit_u decimal (12,2)
,class_supplier_count decimal (12,2)
,dept_supplier_count decimal (12,2)
,loc_supplier_count decimal (12,2)
,plan_cycle_lag_1 int
,sales_plan_lag_1 decimal (12,2)
,bop_plan_lag_1 decimal (12,2)
,rcpt_plan_lag_1 decimal (12,2)
,uncapped_bop_lag_1 decimal (12,2)
,plan_cycle_lag_2 int
,sales_plan_lag_2 decimal (12,2)
,bop_plan_lag_2 decimal (12,2)
,rcpt_plan_lag_2 decimal (12,2)
,uncapped_bop_lag_2 decimal (12,2)
,location_gross_sales_z_score decimal (12,4)
,store_grade varchar(255)
,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
primary index(pkey)
partition by RANGE_N(month_idnt BETWEEN 202301 AND 203001)
;


COMMENT ON  {location_inventory_tracking_t2_schema}.location_inventory_tracking_rack IS 'Location-Dept-Class-Month aggregation at past, current and + 5 months to aid location allocators in daily process. Also provide Inventory Positioning leadership visibility via Inventory Health dashboard. Specific to RACK';

SET QUERY_BAND = NONE FOR SESSION;