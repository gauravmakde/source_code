--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_rms_cost_rtv_from_kafka_to_teradata.sql
-- Author            : Tetiana Ivchyk
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-merchandise-return-to-vendor-analytical-avro using SQL API CODE
-- Source topic      : inventory-merchandise-return-to-vendor-analytical-avro
-- Object model      : MerchandiseReturnToVendor
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-09-25  Tetiana Ivchyk  FA-9950: NAP_ASCP_DW_rms_cost_rtv_S to ISF
-- 2023-11-20  Tetiana Ivchyk  FA-10681 IsF: ascp_rms_cost_rtv to App08983
-- 2024-03-05  Maksym Pochepets  FA-11688: NSK Airflow migration
--*************************************************************************************************************************************


create temporary view staging AS
	select
		returntovendornumber,
		externalreferencenumber,
		fromlocation,
		vendornumber,
		vendoraddress,
		returnauthorizationnumber,
		comments,
		createtime,
		latestupdatetime,
		latestcanceltime,
		headers.SystemTime,
		explode(returntovendordetails) detail
	from kafka_inventory_merchandise_return_to_vendor_analytical_avro;


create temporary view kafka_inventory_merchandise_return_to_vendor_analytical_avro_cnt as
	select
		count(*) as total_cnt
	from kafka_inventory_merchandise_return_to_vendor_analytical_avro;


create temporary view ranked_data as
	select
		returntovendornumber,
		externalreferencenumber,
		fromlocation,
		vendornumber,
		vendoraddress,
		returnauthorizationnumber,
		comments,
		createtime,
		latestupdatetime,
		latestcanceltime,
		detail,
		row_number() OVER (partition by returntovendornumber, detail.returntovendordetail.product.id order by SystemTime desc) row_num
		from staging;


create temporary view final_data as
	select
		coalesce(returntovendornumber,'') as return_to_vendor_number,
		externalreferencenumber external_reference_number,
		fromlocation.facility from_location_facility,
		fromlocation.logical from_location_logical,
		vendornumber vendor_number,
		vendoraddress.id vendor_address_id,
		vendoraddress.firstname.value vendor_address_first_name_tokenized,
		vendoraddress.lastname.value vendor_address_last_name_tokenized,
		vendoraddress.middlename.value vendor_address_middle_name_tokenized,
		vendoraddress.line1.value vendor_address_line1_tokenized,
		vendoraddress.line2.value vendor_address_line2_tokenized,
		vendoraddress.line3.value vendor_address_line3_tokenized,
		vendoraddress.city vendor_address_city,
		vendoraddress.state vendor_address_state,
		vendoraddress.postalcode.value vendor_address_postal_code_tokenized,
		vendoraddress.coarsepostalcode vendor_address_coarse_postal_code,
		vendoraddress.countrycode vendor_address_country_code,
		vendoraddress.maskaddress vendor_address_mask_address,
		regexp_replace(returnauthorizationnumber, '[\\r\\n]', ' ') returns_authorization_number,
		regexp_replace(comments, '[\\r\\n]', ' ') comments,
		createtime create_time,
		latestupdatetime latest_update_time,
		latestcanceltime latest_cancel_time,
		cast(detail.returntovendordetail.product.id as string) as detail_product_id,
		detail.returntovendordetail.product.idtype detail_product_id_type,
		detail.returntovendordetail.quantity detail_quantity,
		detail.returntovendordetail.disposition detail_disposition,
		regexp_replace(detail.returntovendordetail.reasoncode, '[\\r\\n]', ' ') detail_reason_code,
		detail.latestupdatedtime detail_latest_update_time,
		detail.latestcanceledtime detail_latest_cancel_time,
		detail.shippedtime detail_shipped_time
	from ranked_data
	where row_num = 1
		and returntovendornumber <> ''
		and detail.returntovendordetail.product.id <> ''
		and detail.returntovendordetail.product.idtype = 'RMS';



---Writing Error Data to S3 in csv format
-- insert overwrite table rms_cost_rtv_err
-- partition(year, month, day, hour)
-- select /*+ COALESCE(1) */
-- 	return_to_vendor_number,
-- 	external_reference_number,
-- 	from_location_facility,
-- 	from_location_logical,
-- 	vendor_number,
-- 	vendor_address_id,
-- 	vendor_address_first_name_tokenized,
-- 	vendor_address_last_name_tokenized,
-- 	vendor_address_middle_name_tokenized,
-- 	vendor_address_line1_tokenized,
-- 	vendor_address_line2_tokenized,
-- 	vendor_address_line3_tokenized,
-- 	vendor_address_city,
-- 	vendor_address_state,
-- 	vendor_address_postal_code_tokenized,
-- 	vendor_address_coarse_postal_code,
-- 	vendor_address_country_code,
-- 	vendor_address_mask_address,
-- 	returns_authorization_number,
-- 	comments,
-- 	create_time,
-- 	latest_update_time,
-- 	latest_cancel_time,
-- 	detail_product_id,
-- 	detail_product_id_type,
-- 	detail_quantity,
-- 	detail_disposition,
-- 	detail_reason_code,
-- 	detail_latest_update_time,
-- 	detail_latest_cancel_time,
-- 	detail_shipped_time,
-- 	substr(current_date, 1, 4) as year,
-- 	substr(current_date, 6, 2) as month,
-- 	substr(current_date, 9, 2) as day,
-- 	substr(current_timestamp, 12, 2) as hour
-- from final_data
-- where
-- 	return_to_vendor_number is null or return_to_vendor_number = '' or return_to_vendor_number = '""'
-- 	or detail_product_id is null or detail_product_id = '' or detail_product_id = '""';



---Writing Kafka Data to Teradata staging table
insert overwrite table rms_cost_rtv_ldg_table_csv
select
	return_to_vendor_number,
	external_reference_number,
	from_location_facility,
	from_location_logical,
	vendor_number,
	vendor_address_id,
	vendor_address_first_name_tokenized,
	vendor_address_last_name_tokenized,
	vendor_address_middle_name_tokenized,
	vendor_address_line1_tokenized,
	vendor_address_line2_tokenized,
	vendor_address_line3_tokenized,
	vendor_address_city,
	vendor_address_state,
	vendor_address_postal_code_tokenized,
	vendor_address_coarse_postal_code,
	vendor_address_country_code,
	vendor_address_mask_address,
	returns_authorization_number,
	comments,
	create_time,
	latest_update_time,
	latest_cancel_time,
	detail_product_id,
	detail_product_id_type,
	detail_quantity,
	detail_disposition,
	detail_reason_code,
	detail_latest_update_time,
	detail_latest_cancel_time,
	detail_shipped_time
from final_data
where
	return_to_vendor_number <> '' and return_to_vendor_number <> '""'
	and detail_product_id <> '' and detail_product_id <> '""';



---Writing Spark job count metrics
insert into table processed_data_spark_log_table
select
	isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	cast(metric_value as string),
	cast(metric_tmstp as string)
from (
	select
		'ascp_rms_cost_rtv_16780_TECH_SC_NAP_insights' as isf_dag_nm,
		'Loading to csv' as step_nm,
		'RMS_COST_RTV_LDG' as tbl_nm,
		'PROCESSED_ROWS_CNT' as metric_nm,
		cast(count(*) as string) as metric_value,
		cast(current_timestamp() as string) as metric_tmstp
	from final_data
	where
		return_to_vendor_number <> '' and return_to_vendor_number <> '""'
		and detail_product_id <> '' and detail_product_id <> '""'
	union all
	select
		'ascp_rms_cost_rtv_16780_TECH_SC_NAP_insights' as isf_dag_nm,
		'Reading from kafka' as step_nm,
		cast (null as string) as tbl_nm,
		'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
		cast(total_cnt as string) as metric_value,
		cast(current_timestamp() as string) as metric_tmstp
	from kafka_inventory_merchandise_return_to_vendor_analytical_avro_cnt
	 ) a;
