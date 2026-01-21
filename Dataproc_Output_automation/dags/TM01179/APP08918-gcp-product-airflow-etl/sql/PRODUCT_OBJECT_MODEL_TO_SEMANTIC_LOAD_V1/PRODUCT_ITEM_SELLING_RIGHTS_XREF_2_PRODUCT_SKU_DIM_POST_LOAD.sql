
TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw;



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw 
(rms_sku_num, 
epm_sku_num, 
channel_country, 
sku_short_desc, 
sku_desc,
 web_sku_num, 
 brand_label_num, 
 brand_label_display_name, 
 rms_style_num, 
 epm_style_num, 
 partner_relationship_num,
 partner_relationship_type_code, 
 web_style_num, 
 style_desc, 
 epm_choice_num, 
 supp_part_num, 
 prmy_supp_num,
 manufacturer_num, 
 sbclass_num, 
 sbclass_desc, 
 class_num, 
 class_desc, 
 dept_num, 
 dept_desc, 
 grp_num, 
 grp_desc, 
 div_num,
 div_desc, 
 cmpy_num, 
 cmpy_desc, 
 color_num, 
 color_desc, 
 nord_display_color, 
 nrf_size_code, 
 size_1_num, 
 size_1_desc,
 size_2_num, 
 size_2_desc, 
 supp_color, 
 supp_size, 
 brand_name, 
 return_disposition_code, 
 return_disposition_desc,
 selling_status_code, 
 selling_status_desc, 
 live_date, 
 drop_ship_eligible_ind, 
 sku_type_code, 
 sku_type_desc,
 hazardous_material_class_desc, 
 hazardous_material_class_code, 
 fulfillment_type_code, 
 selling_channel_eligibility_list,
 smart_sample_ind, 
 gwp_ind, 
 msrp_amt, 
 msrp_currency_code, 
 npg_ind, 
 order_quantity_multiple, 
 fp_forecast_eligible_ind,
 fp_item_planning_eligible_ind, 
 fp_replenishment_eligible_ind, 
 op_forecast_eligible_ind, 
 op_item_planning_eligible_ind,
 op_replenishment_eligible_ind, 
 size_range_desc, 
 size_sequence_num, 
 size_range_code, 
 eff_begin_tmstp, 
 eff_end_tmstp,
 eff_begin_tmstp_tz,
 eff_end_tmstp_tz )


 with src_1 as
 (select  rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp,eff_end_tmstp
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT  rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY  rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from 
				(SELECT SRC_2.*,
        COALESCE(eff_begin_tmstp, COALESCE(MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
          ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), TIMESTAMP'9999-12-31 23:59:59.999999+00:00')) AS eff_end_tmstp
       FROM (SELECT DISTINCT sku.rms_sku_num,
          sku.epm_sku_num,
          sku.channel_country,
          sku.sku_short_desc,
          sku.sku_desc,
          sku.web_sku_num,
          sku.brand_label_num,
          sku.brand_label_display_name,
          sku.rms_style_num,
          sku.epm_style_num,
          sku.partner_relationship_num,
          sku.partner_relationship_type_code,
          sku.web_style_num,
          sku.epm_choice_num,
          sku.supp_part_num,
          sku.prmy_supp_num,
          sku.manufacturer_num,
          sku.nrf_size_code,
          sku.size_1_num,
          sku.size_1_desc,
          sku.size_2_num,
          sku.size_2_desc,
          sku.supp_color,
          sku.supp_size,
          sku.return_disposition_code,
          sku.return_disposition_desc,
          sku.drop_ship_eligible_ind,
          sku.hazardous_material_class_desc,
          sku.hazardous_material_class_code,
          sku.fulfillment_type_code,
          sku.smart_sample_ind,
          sku.gwp_ind,
          sku.msrp_amt,
          sku.msrp_currency_code,
          sku.npg_ind,
          sku.order_quantity_multiple,
          sku.fp_forecast_eligible_ind,
          sku.fp_item_planning_eligible_ind,
          sku.fp_replenishment_eligible_ind,
          sku.op_forecast_eligible_ind,
          sku.op_item_planning_eligible_ind,
          sku.op_replenishment_eligible_ind,
          sku.size_range_desc,
          sku.size_sequence_num,
          sku.size_range_code,
           CASE
           WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
           THEN 'S1'
           WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
              ))
           THEN 'T1'
           WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
           THEN 'C3'
           ELSE NULL
           END AS derived_selling_status_code,
           CASE
           WHEN LOWER(CASE
              WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
              THEN 'S1'
              WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'),
                LOWER('4060'))
              THEN 'T1'
              WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
              THEN 'C3'
              ELSE NULL
              END) = LOWER('S1')
           THEN 'Unsellable'
           WHEN LOWER(CASE
              WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
              THEN 'S1'
              WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'),
                LOWER('4060'))
              THEN 'T1'
              WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
              THEN 'C3'
              ELSE NULL
              END) = LOWER('C3')
           THEN 'Sellable'
           WHEN LOWER(CASE
              WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
              THEN 'S1'
              WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'),
                LOWER('4060'))
              THEN 'T1'
              WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
              THEN 'C3'
              ELSE NULL
              END) = LOWER('T1')
           THEN 'Dropship'
           ELSE NULL
           END AS selling_status_desc,
           CASE
           WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
           THEN 'S1'
           WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
              ))
           THEN 'T1'
           WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
           THEN 'C3'
           ELSE NULL
           END AS selling_status_code,
          xref.live_date,
          xref.selling_channel_eligibility_list,
          xref.eff_begin_tmstp
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg AS xref
          INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS sku ON LOWER(xref.rms_sku_num) = LOWER(sku.rms_sku_num) AND LOWER(xref
             .channel_country) = LOWER(sku.channel_country)
          LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg AS delta_check 
          ON LOWER(xref.rms_sku_num) = LOWER(delta_check
               .rms_sku_num) 
               AND LOWER(xref.channel_country) = LOWER(delta_check.channel_country) 
               AND xref.eff_begin_tmstp
              = delta_check.sr_eff_begin_tmstp AND xref.eff_end_tmstp = delta_check.sr_eff_end_tmstp
         WHERE 
         RANGE_OVERLAPS(RANGE(SKU.eff_begin_tmstp, SKU.eff_end_tmstp) , RANGE(XREF.eff_begin_tmstp, XREF.eff_end_tmstp))
				AND 
         delta_check.rms_sku_num IS NOT NULL
         ) AS SRC_2
       QUALIFY eff_begin_tmstp < eff_end_tmstp
			 )) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,range_group
ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, eff_begin_tmstp) ),




 src_3 as
 (select rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, MIN(eff_begin_tmstp1) AS eff_begin_tmstp, MAX(eff_end_tmstp1) AS eff_end_tmstp,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_desc,subdivision_num,subdivision_short_name ,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ORDER BY eff_begin_tmstp1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT SRC0.rms_sku_num,SRC0.epm_sku_num,SRC0.channel_country,SRC0.sku_short_desc,SRC0.sku_desc,SRC0.web_sku_num,SRC0.brand_label_num,SRC0.brand_label_display_name,SRC0.rms_style_num,SRC0.epm_style_num,SRC0.partner_relationship_num,SRC0.partner_relationship_type_code,SRC0.web_style_num,styl.style_desc,SRC0.epm_choice_num,SRC0.supp_part_num,SRC0.prmy_supp_num,SRC0.manufacturer_num,styl.sbclass_num,styl.sbclass_desc,styl.class_num,styl.class_desc,styl.dept_num,styl.dept_desc,dept.subdivision_num,dept.subdivision_short_name,dept.division_num,dept.division_short_name,choice.nrf_color_num,choice.color_desc,choice.nord_display_color,SRC0.nrf_size_code,SRC0.size_1_num,SRC0.size_1_desc,SRC0.size_2_num,SRC0.size_2_desc,SRC0.supp_color,SRC0.supp_size,styl.vendor_label_name,SRC0.return_disposition_code,SRC0.return_disposition_desc,SRC0.selling_status_code,SRC0.selling_status_desc,SRC0.live_date,SRC0.drop_ship_eligible_ind,SRC0.hazardous_material_class_desc,SRC0.hazardous_material_class_code,SRC0.fulfillment_type_code,SRC0.selling_channel_eligibility_list,SRC0.smart_sample_ind,SRC0.gwp_ind,SRC0.msrp_amt,SRC0.msrp_currency_code,SRC0.npg_ind,SRC0.order_quantity_multiple,SRC0.fp_forecast_eligible_ind,SRC0.fp_item_planning_eligible_ind,SRC0.fp_replenishment_eligible_ind,SRC0.op_forecast_eligible_ind,SRC0.op_item_planning_eligible_ind,SRC0.op_replenishment_eligible_ind,SRC0.size_range_desc,SRC0.size_sequence_num,SRC0.size_range_code,
            CASE 
                WHEN LAG(styl.eff_end_tmstp_utc) OVER (PARTITION BY SRC0.rms_sku_num,SRC0.epm_sku_num,src0.channel_country,SRC0.sku_short_desc,SRC0.sku_desc,SRC0.web_sku_num,SRC0.brand_label_num,SRC0.brand_label_display_name,SRC0.rms_style_num,SRC0.epm_style_num,SRC0.partner_relationship_num,SRC0.partner_relationship_type_code,SRC0.web_style_num,styl.style_desc,SRC0.epm_choice_num,SRC0.supp_part_num,SRC0.prmy_supp_num,SRC0.manufacturer_num,styl.sbclass_num,styl.sbclass_desc,styl.class_num,styl.class_desc,styl.dept_num,styl.dept_desc,dept.subdivision_num,dept.subdivision_short_name ,dept.division_num,dept.division_short_name,choice.nrf_color_num,choice.color_desc,choice.nord_display_color,SRC0.nrf_size_code,SRC0.size_1_num,SRC0.size_1_desc,SRC0.size_2_num,SRC0.size_2_desc,SRC0.supp_color,SRC0.supp_size,styl.vendor_label_name,SRC0.return_disposition_code,SRC0.return_disposition_desc,SRC0.selling_status_code,SRC0.selling_status_desc,SRC0.live_date,SRC0.drop_ship_eligible_ind,SRC0.hazardous_material_class_desc,SRC0.hazardous_material_class_code,SRC0.fulfillment_type_code,SRC0.selling_channel_eligibility_list,SRC0.smart_sample_ind,SRC0.gwp_ind,SRC0.msrp_amt,SRC0.msrp_currency_code,SRC0.npg_ind,SRC0.order_quantity_multiple,SRC0.fp_forecast_eligible_ind,SRC0.fp_item_planning_eligible_ind,SRC0.fp_replenishment_eligible_ind,SRC0.op_forecast_eligible_ind,SRC0.op_item_planning_eligible_ind,SRC0.op_replenishment_eligible_ind,SRC0.size_range_desc,SRC0.size_sequence_num,SRC0.size_range_code ORDER BY styl.eff_begin_tmstp_utc) >= 
                DATE_SUB(styl.eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
						styl.eff_begin_tmstp_utc as eff_begin_tmstp1,
						styl.eff_end_tmstp_utc as eff_end_tmstp1,
             COALESCE(SAFE.RANGE_INTERSECT(SAFE.RANGE_INTERSECT((CASE WHEN styl.epm_style_num>0 THEN SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(styl.eff_begin_tmstp_utc,styl.eff_end_tmstp_utc)) ELSE SRC0.eff_period END)
	     ,
		 (CASE WHEN dept.dept_num>0 THEN SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(dept.eff_begin_tmstp_utc,dept.eff_end_tmstp_utc)) ELSE SRC0.eff_period END))
	     ,
		 CASE WHEN choice.epm_choice_num>0 THEN SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(choice.eff_begin_tmstp_utc,choice.eff_end_tmstp_utc)) ELSE SRC0.eff_period END
	     )
	          , SRC0.eff_period ) AS  eff_period 
         from 
				 (SELECT 
     --normalize
      rms_sku_num,
        epm_sku_num,
        channel_country ,
        sku_short_desc,
        sku_desc,
        web_sku_num,
        brand_label_num,
        brand_label_display_name,
        rms_style_num,
        epm_style_num ,
        partner_relationship_num,
        partner_relationship_type_code,
        web_style_num,
        epm_choice_num,
        supp_part_num ,
        prmy_supp_num ,
        manufacturer_num,
        nrf_size_code,
        size_1_num,
        size_1_desc,
        size_2_num,
        size_2_desc,
        supp_color,
        supp_size,
        return_disposition_code,
        return_disposition_desc,
        selling_status_code,
        selling_status_desc,
        live_date,
        drop_ship_eligible_ind,
        hazardous_material_class_desc,
        hazardous_material_class_code,
        fulfillment_type_code,
        selling_channel_eligibility_list,
        smart_sample_ind,
        gwp_ind,
        msrp_amt,
        msrp_currency_code,
        npg_ind,
        order_quantity_multiple,
        fp_forecast_eligible_ind,
        fp_item_planning_eligible_ind,
        fp_replenishment_eligible_ind,
        op_forecast_eligible_ind,
        op_item_planning_eligible_ind,
        op_replenishment_eligible_ind,
        size_range_desc,
        size_sequence_num,
        size_range_code,
				range(eff_begin_tmstp, eff_end_tmstp) AS eff_period
				from SRC_1
	) AS SRC0
      
      LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS styl 
      ON SRC0.epm_style_num = styl.epm_style_num 
      AND LOWER(SRC0.channel_country) = LOWER(styl.channel_country)
       AND RANGE_OVERLAPS(src0.eff_period,RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc)) 




      LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS dept 
      ON dept.dept_num = CAST(TRIM(FORMAT('%11d', styl.dept_num)) AS FLOAT64)
         AND (styl.eff_begin_tmstp_utc > dept.eff_begin_tmstp_utc 
         AND styl.eff_begin_tmstp_utc < dept.eff_end_tmstp_utc OR styl.eff_begin_tmstp_utc
             < dept.eff_begin_tmstp_utc 
             AND styl.eff_end_tmstp_utc > dept.eff_begin_tmstp_utc OR styl.eff_begin_tmstp_utc = dept.eff_begin_tmstp_utc
            AND (styl.eff_end_tmstp_utc = dept.eff_end_tmstp_utc OR styl.eff_end_tmstp_utc <> dept.eff_end_tmstp_utc))
AND RANGE_OVERLAPS(src0.eff_period , RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc) )



      LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_choice_dim_hist AS choice 
      ON SRC0.epm_choice_num = choice.epm_choice_num 
      AND
          LOWER(SRC0.channel_country) = LOWER(choice.channel_country) 
          AND (styl.eff_begin_tmstp_utc > choice.eff_begin_tmstp_utc
              AND styl.eff_begin_tmstp_utc < choice.eff_end_tmstp_utc OR styl.eff_begin_tmstp_utc < choice.eff_begin_tmstp_utc 
              AND styl
             .eff_end_tmstp_utc > choice.eff_begin_tmstp_utc OR styl.eff_begin_tmstp_utc = choice.eff_begin_tmstp_utc 
             AND (styl.eff_end_tmstp_utc
               = choice.eff_end_tmstp_utc OR styl.eff_end_tmstp_utc <> choice.eff_end_tmstp_utc)) AND (dept.eff_begin_tmstp_utc > choice
            .eff_begin_tmstp_utc AND dept.eff_begin_tmstp_utc < choice.eff_end_tmstp_utc OR dept.eff_begin_tmstp_utc < choice.eff_begin_tmstp_utc
             AND dept.eff_end_tmstp_utc > choice.eff_begin_tmstp_utc OR dept.eff_begin_tmstp_utc = choice.eff_begin_tmstp_utc AND (dept
             .eff_end_tmstp_utc = choice.eff_end_tmstp_utc OR dept.eff_end_tmstp_utc <> choice.eff_end_tmstp_utc))

              AND RANGE_OVERLAPS(src0.eff_period , RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc))

							 ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, eff_begin_tmstp) )



SELECT 
rms_sku_num,
  epm_sku_num,
  channel_country,
  sku_short_desc,
  sku_desc,
  web_sku_num,
  brand_label_num,
  brand_label_display_name,
  rms_style_num,
  epm_style_num,
  partner_relationship_num,
  partner_relationship_type_code,
  web_style_num,
  style_desc,
  epm_choice_num,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  sbclass_num,
  sbclass_desc,
  class_num,
  class_desc,
  dept_num,
  dept_desc,
  grp_num,
  grp_desc,
  div_num,
  div_desc,
  CAST(cmpy_num AS SMALLINT) AS cmpy_num,
  cmpy_desc,
  color_num,
  color_desc,
  nord_display_color,
  nrf_size_code,
  size_1_num,
  size_1_desc,
  size_2_num,
  size_2_desc,
  supp_color,
  supp_size,
  brand_name,
  return_disposition_code,
  return_disposition_desc,
  selling_status_code,
  selling_status_desc,
  live_date,
  drop_ship_eligible_ind,
  sku_type_code,
  sku_type_desc,
  hazardous_material_class_desc,
  hazardous_material_class_code,
  fulfillment_type_code,
  selling_channel_eligibility_list,
  smart_sample_ind,
  gwp_ind,
  msrp_amt,
  msrp_currency_code,
  npg_ind,
  order_quantity_multiple,
  fp_forecast_eligible_ind,
  fp_item_planning_eligible_ind,
  fp_replenishment_eligible_ind,
  op_forecast_eligible_ind,
  op_item_planning_eligible_ind,
  op_replenishment_eligible_ind,
  size_range_desc,
  size_sequence_num,
  size_range_code,
  min(RANGE_START(eff_period))  AS eff_begin_tmstp, 
  max(RANGE_END(eff_period )) AS eff_end_tmstp,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE` (CAST(min(RANGE_START(eff_period)) AS STRING)) AS eff_begin_tmstp_tz,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE` (CAST(max(RANGE_END(eff_period )) AS STRING)) AS eff_end_tmstp_tz
from  (
    --inner normalize
            SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT SRC.rms_sku_num,SRC.epm_sku_num,SRC.channel_country,SRC.sku_short_desc,SRC.sku_desc,SRC.web_sku_num,SRC.brand_label_num,SRC.brand_label_display_name,SRC.rms_style_num,SRC.epm_style_num,SRC.partner_relationship_num,SRC.partner_relationship_type_code,SRC.web_style_num,SRC.style_desc,SRC.epm_choice_num,SRC.supp_part_num,SRC.prmy_supp_num,SRC.manufacturer_num,SRC.sbclass_num,SRC.sbclass_desc,SRC.class_num,SRC.class_desc,SRC.dept_num,SRC.dept_desc,SRC.grp_num,SRC.grp_desc,SRC.div_num,SRC.div_desc,SRC.cmpy_num,SRC.cmpy_desc,SRC.color_num,SRC.color_desc,SRC.nord_display_color,SRC.nrf_size_code,SRC.size_1_num,SRC.size_1_desc,SRC.size_2_num,SRC.size_2_desc,SRC.supp_color,SRC.supp_size,SRC.brand_name,SRC.return_disposition_code,SRC.return_disposition_desc,SRC.selling_status_code,SRC.selling_status_desc,SRC.live_date,SRC.drop_ship_eligible_ind,SRC.sku_type_code,SRC.sku_type_desc,SRC.hazardous_material_class_desc,SRC.hazardous_material_class_code,SRC.fulfillment_type_code,SRC.selling_channel_eligibility_list,SRC.smart_sample_ind,SRC.gwp_ind,SRC.msrp_amt,SRC.msrp_currency_code,SRC.npg_ind,SRC.order_quantity_multiple,SRC.fp_forecast_eligible_ind,SRC.fp_item_planning_eligible_ind,SRC.fp_replenishment_eligible_ind,SRC.op_forecast_eligible_ind,SRC.op_item_planning_eligible_ind,SRC.op_replenishment_eligible_ind,SRC.size_range_desc,SRC.size_sequence_num,SRC.size_range_code, TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY SRC.rms_sku_num,SRC.epm_sku_num,SRC.channel_country,SRC.sku_short_desc,SRC.sku_desc,SRC.web_sku_num,SRC.brand_label_num,SRC.brand_label_display_name,SRC.rms_style_num,SRC.epm_style_num,SRC.partner_relationship_num,SRC.partner_relationship_type_code,SRC.web_style_num,SRC.style_desc,SRC.epm_choice_num,SRC.supp_part_num,SRC.prmy_supp_num,SRC.manufacturer_num,SRC.sbclass_num,SRC.sbclass_desc,SRC.class_num,SRC.class_desc,SRC.dept_num,SRC.dept_desc,SRC.grp_num,SRC.grp_desc,SRC.div_num,SRC.div_desc,SRC.cmpy_num,SRC.cmpy_desc,SRC.color_num,SRC.color_desc,SRC.nord_display_color,SRC.nrf_size_code,SRC.size_1_num,SRC.size_1_desc,SRC.size_2_num,SRC.size_2_desc,SRC.supp_color,SRC.supp_size,SRC.brand_name,SRC.return_disposition_code,SRC.return_disposition_desc,SRC.selling_status_code,SRC.selling_status_desc,SRC.live_date,SRC.drop_ship_eligible_ind,SRC.sku_type_code,SRC.sku_type_desc,SRC.hazardous_material_class_desc,SRC.hazardous_material_class_code,SRC.fulfillment_type_code,SRC.selling_channel_eligibility_list,SRC.smart_sample_ind,SRC.gwp_ind,SRC.msrp_amt,SRC.msrp_currency_code,SRC.npg_ind,SRC.order_quantity_multiple,SRC.fp_forecast_eligible_ind,SRC.fp_item_planning_eligible_ind,SRC.fp_replenishment_eligible_ind,SRC.op_forecast_eligible_ind,SRC.op_item_planning_eligible_ind,SRC.op_replenishment_eligible_ind,SRC.size_range_desc,SRC.size_sequence_num,SRC.size_range_code ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE(SAFE.RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)),SRC.eff_period) AS eff_period,
             FROM
	(SELECT  
      rms_sku_num,
      epm_sku_num,
      channel_country,
      sku_short_desc,
      sku_desc,
      web_sku_num,
      brand_label_num,
      brand_label_display_name,
      rms_style_num,
      epm_style_num,
      partner_relationship_num,
      partner_relationship_type_code,
      web_style_num,
      style_desc,
      epm_choice_num,
      supp_part_num,
      prmy_supp_num,
      manufacturer_num,
      sbclass_num,
      sbclass_desc,
      class_num,
      class_desc,
      dept_num,
      dept_desc,
      subdivision_num AS grp_num,
      subdivision_short_name AS grp_desc,
      division_num AS div_num,
      division_short_name AS div_desc,
      '1000' AS cmpy_num,
      'Nordstrom' AS cmpy_desc,
      nrf_color_num AS color_num,
      color_desc,
      nord_display_color,
      nrf_size_code,
      size_1_num,
      size_1_desc,
      size_2_num,
      size_2_desc,
      supp_color,
      supp_size,
      vendor_label_name AS brand_name,
      return_disposition_code,
      return_disposition_desc,
      selling_status_code,
      selling_status_desc,
      live_date,
      drop_ship_eligible_ind,
      'f' AS sku_type_code,
      'Fashion Sku' AS sku_type_desc,
      hazardous_material_class_desc,
      hazardous_material_class_code,
      fulfillment_type_code,
      selling_channel_eligibility_list,
      smart_sample_ind,
      gwp_ind,
      msrp_amt,
      msrp_currency_code,
      npg_ind,
      order_quantity_multiple,
      fp_forecast_eligible_ind,
      fp_item_planning_eligible_ind,
      fp_replenishment_eligible_ind,
      op_forecast_eligible_ind,
      op_item_planning_eligible_ind,
      op_replenishment_eligible_ind,
      size_range_desc,
      size_sequence_num,
      size_range_code,
			eff_period
     FROM src_3) AS SRC
    LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS tgt 
    ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(SRC
       .channel_country) = LOWER(tgt.channel_country)
AND RANGE_OVERLAPS(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))
    WHERE (tgt.rms_sku_num IS NULL
    OR (SRC.epm_sku_num <> tgt.epm_sku_num
    OR (tgt.epm_sku_num IS NULL AND SRC.epm_sku_num IS NOT NULL)
    OR (SRC.epm_sku_num IS NULL AND tgt.epm_sku_num IS NOT NULL))

    OR (LOWER(SRC.selling_status_code) <> LOWER(tgt.selling_status_code)
    OR (tgt.selling_status_code IS NULL AND SRC.selling_status_code IS NOT NULL)
    OR (SRC.selling_status_code IS NULL AND tgt.selling_status_code IS NOT NULL))

    OR (SRC.live_date <> tgt.live_date
    OR (tgt.live_date IS NULL AND SRC.live_date IS NOT NULL)
    OR (SRC.live_date IS NULL AND tgt.live_date IS NOT NULL))

    OR (LOWER(SRC.selling_channel_eligibility_list) <> LOWER(tgt.selling_channel_eligibility_list)
    OR (tgt.selling_channel_eligibility_list IS NULL AND SRC.selling_channel_eligibility_list IS NOT NULL)
    OR (SRC.selling_channel_eligibility_list IS NULL AND tgt.selling_channel_eligibility_list IS NOT NULL))) 
		 )as   ordered_data
) AS grouped_data)
GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code;


BEGIN
BEGIN TRANSACTION;

--SEQUENCED VALIDTIME
--update solution
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
    WHERE epm_sku_num = tgt.epm_sku_num 
    AND LOWER(channel_country) = LOWER(tgt.channel_country)
    AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp);

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num = tgt.epm_sku_num 
    AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num = tgt.epm_sku_num 
    AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;


insert into `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
with tbl as 
(SELECT 
tgt.*,
cast(src.eff_begin_tmstp as timestamp) AS src_eff_begin_tmstp, 
cast(src.eff_end_tmstp as timestamp) AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
 inner join `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
 on src.epm_sku_num = tgt.epm_sku_num 
 AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp ), tgt_eff_begin_tmstp, cast(src_eff_begin_tmstp as timestamp) FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), cast(src_eff_end_tmstp as timestamp), tgt_eff_end_tmstp FROM tbl;

delete from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt where exists (select 1 
from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
where src.epm_sku_num = tgt.epm_sku_num 
and tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);




--SEQUENCED VALIDTIME
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
    WHERE epm_sku_num <> tgt.epm_sku_num 
    AND LOWER(channel_country) = LOWER(tgt.channel_country) 
    AND LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(tgt.sku_type_code) = LOWER('P')
    AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp);

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num <> tgt.epm_sku_num 
    AND LOWER(src.channel_country) = LOWER(tgt.channel_country) 
    AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(tgt.sku_type_code) = LOWER('P')
    AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp ;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num <> tgt.epm_sku_num 
    AND LOWER(src.channel_country) = LOWER(tgt.channel_country) 
    AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(tgt.sku_type_code) = LOWER('P')
    AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

insert into `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
with tbl as 
(SELECT 
tgt.*,
cast(src.eff_begin_tmstp as timestamp) AS src_eff_begin_tmstp, 
cast(src.eff_end_tmstp as timestamp) AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
 inner join `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
 on src.epm_sku_num = tgt.epm_sku_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp ), tgt_eff_begin_tmstp, cast(src_eff_begin_tmstp as timestamp) FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), cast(src_eff_end_tmstp as timestamp), tgt_eff_end_tmstp FROM tbl;

delete from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt where exists (select 1 
from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
where src.epm_sku_num = tgt.epm_sku_num and tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
 web_sku_num, brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num,
 partner_relationship_type_code, web_style_num, style_desc, epm_choice_num, supp_part_num, prmy_supp_num,
 manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num,
 div_desc, cmpy_num, cmpy_desc, color_num, color_desc, nord_display_color, nrf_size_code, size_1_num, size_1_desc,
 size_2_num, size_2_desc, supp_color, supp_size, brand_name, return_disposition_code, return_disposition_desc,
 selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, sku_type_code, sku_type_desc,
 pack_orderable_code, pack_orderable_desc, pack_sellable_code, pack_sellable_desc, pack_simple_code, pack_simple_desc,
 display_seq_1, display_seq_2, hazardous_material_class_desc, hazardous_material_class_code, fulfillment_type_code,
 selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, msrp_currency_code, npg_ind,
 order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, fp_replenishment_eligible_ind,
 op_forecast_eligible_ind, op_item_planning_eligible_ind, op_replenishment_eligible_ind, size_range_desc,
 size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  epm_sku_num,
  channel_country,
  sku_short_desc,
  sku_desc,
  web_sku_num,
  brand_label_num,
  brand_label_display_name,
  rms_style_num,
  epm_style_num,
  partner_relationship_num,
  partner_relationship_type_code,
  web_style_num,
  style_desc,
  epm_choice_num,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  sbclass_num,
  sbclass_desc,
  class_num,
  class_desc,
  dept_num,
  dept_desc,
  grp_num,
  grp_desc,
  div_num,
  div_desc,
  cmpy_num,
  cmpy_desc,
  color_num,
  color_desc,
  nord_display_color,
  nrf_size_code,
  size_1_num,
  size_1_desc,
  size_2_num,
  size_2_desc,
  supp_color,
  supp_size,
  brand_name,
  return_disposition_code,
  return_disposition_desc,
  selling_status_code,
  selling_status_desc,
  live_date,
  drop_ship_eligible_ind,
  sku_type_code,
  sku_type_desc,
  pack_orderable_code,
  pack_orderable_desc,
  pack_sellable_code,
  pack_sellable_desc,
  pack_simple_code,
  pack_simple_desc,
  display_seq_1,
  display_seq_2,
  hazardous_material_class_desc,
  hazardous_material_class_code,
  fulfillment_type_code,
  selling_channel_eligibility_list,
  smart_sample_ind,
  gwp_ind,
  msrp_amt,
  msrp_currency_code,
  npg_ind,
  order_quantity_multiple,
  fp_forecast_eligible_ind,
  fp_item_planning_eligible_ind,
  fp_replenishment_eligible_ind,
  op_forecast_eligible_ind,
  op_item_planning_eligible_ind,
  op_replenishment_eligible_ind,
  size_range_desc,
  size_sequence_num,
  size_range_code,
  eff_begin_tmstp,
  eff_end_tmstp,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    epm_sku_num,
    channel_country,
    sku_short_desc,
    sku_desc,
    web_sku_num,
    brand_label_num,
    brand_label_display_name,
    rms_style_num,
    epm_style_num,
    partner_relationship_num,
    partner_relationship_type_code,
    web_style_num,
    style_desc,
    epm_choice_num,
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
    sbclass_num,
    sbclass_desc,
    class_num,
    class_desc,
    dept_num,
    dept_desc,
    grp_num,
    grp_desc,
    div_num,
    div_desc,
    cmpy_num,
    cmpy_desc,
    color_num,
    color_desc,
    nord_display_color,
    nrf_size_code,
    size_1_num,
    size_1_desc,
    size_2_num,
    size_2_desc,
    supp_color,
    supp_size,
    brand_name,
    return_disposition_code,
    return_disposition_desc,
    selling_status_code,
    selling_status_desc,
    live_date,
    drop_ship_eligible_ind,
    sku_type_code,
    sku_type_desc,
    pack_orderable_code,
    pack_orderable_desc,
    pack_sellable_code,
    pack_sellable_desc,
    pack_simple_code,
    pack_simple_desc,
    display_seq_1,
    display_seq_2,
    hazardous_material_class_desc,
    hazardous_material_class_code,
    fulfillment_type_code,
    selling_channel_eligibility_list,
    smart_sample_ind,
    gwp_ind,
    msrp_amt,
    msrp_currency_code,
    npg_ind,
    order_quantity_multiple,
    fp_forecast_eligible_ind,
    fp_item_planning_eligible_ind,
    fp_replenishment_eligible_ind,
    op_forecast_eligible_ind,
    op_item_planning_eligible_ind,
    op_replenishment_eligible_ind,
    size_range_desc,
    size_sequence_num,
    size_range_code,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
     (SELECT batch_id
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
   WHERE epm_sku_num = t3.epm_sku_num
    AND channel_country = t3.channel_country)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);




UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim  AS tgt 
SET
    eff_end_tmstp = SRC.new_end_tmstp ,
    eff_end_tmstp_tz = `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(new_end_tmstp AS STRING))
    FROM 
    (SELECT rms_sku_num, channel_country, epm_sku_num, sku_desc, sku_type_code, eff_begin_tmstp, eff_end_tmstp AS old_eff_tmstp, MIN(eff_begin_tmstp) 
    OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp,
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
        WHERE LOWER(rms_sku_num) <> LOWER('')
        QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country) 
AND SRC.epm_sku_num = tgt.epm_sku_num 
AND SRC.eff_begin_tmstp = tgt.eff_begin_tmstp 
AND SRC.new_end_tmstp > tgt.eff_begin_tmstp;






INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_overlap_stg (new_epm_sku_num, rms_sku_num, epm_sku_num, channel_country,
 sku_short_desc, sku_desc, web_sku_num, brand_label_num, brand_label_display_name, rms_style_num, epm_style_num,
 partner_relationship_num, partner_relationship_type_code, web_style_num, style_desc, epm_choice_num, supp_part_num,
 prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num,
 grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, color_desc, nord_display_color, nrf_size_code, size_1_num
 , size_1_desc, size_2_num, size_2_desc, supp_color, supp_size, brand_name, return_disposition_code,
 return_disposition_desc, selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, sku_type_code,
 sku_type_desc, pack_orderable_code, pack_orderable_desc, pack_sellable_code, pack_sellable_desc, pack_simple_code,
 pack_simple_desc, display_seq_1, display_seq_2, hazardous_material_class_desc, hazardous_material_class_code,
 fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, msrp_currency_code,
 npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind,
 fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, op_replenishment_eligible_ind,
 size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp, new_epm_eff_begin, new_epm_eff_end, old_epm_eff_begin, old_epm_eff_end, process_flag)
(SELECT * FROM (SELECT DISTINCT b.epm_sku_num AS new_epm_sku_num,
  c.rms_sku_num,
  c.epm_sku_num,
  c.channel_country,
  c.sku_short_desc,
  c.sku_desc,
  c.web_sku_num,
  c.brand_label_num,
  c.brand_label_display_name,
  c.rms_style_num,
  c.epm_style_num,
  c.partner_relationship_num,
  c.partner_relationship_type_code,
  c.web_style_num,
  c.style_desc,
  c.epm_choice_num,
  c.supp_part_num,
  c.prmy_supp_num,
  c.manufacturer_num,
  c.sbclass_num,
  c.sbclass_desc,
  c.class_num,
  c.class_desc,
  c.dept_num,
  c.dept_desc,
  c.grp_num,
  c.grp_desc,
  c.div_num,
  c.div_desc,
  c.cmpy_num,
  c.cmpy_desc,
  c.color_num,
  c.color_desc,
  c.nord_display_color,
  c.nrf_size_code,
  c.size_1_num,
  c.size_1_desc,
  c.size_2_num,
  c.size_2_desc,
  c.supp_color,
  c.supp_size,
  c.brand_name,
  c.return_disposition_code,
  c.return_disposition_desc,
  c.selling_status_code,
  c.selling_status_desc,
  c.live_date,
  c.drop_ship_eligible_ind,
  c.sku_type_code,
  c.sku_type_desc,
  c.pack_orderable_code,
  c.pack_orderable_desc,
  c.pack_sellable_code,
  c.pack_sellable_desc,
  c.pack_simple_code,
  c.pack_simple_desc,
  c.display_seq_1,
  c.display_seq_2,
  c.hazardous_material_class_desc,
  c.hazardous_material_class_code,
  c.fulfillment_type_code,
  c.selling_channel_eligibility_list,
  c.smart_sample_ind,
  c.gwp_ind,
  c.msrp_amt,
  c.msrp_currency_code,
  c.npg_ind,
  c.order_quantity_multiple,
  c.fp_forecast_eligible_ind,
  c.fp_item_planning_eligible_ind,
  c.fp_replenishment_eligible_ind,
  c.op_forecast_eligible_ind,
  c.op_item_planning_eligible_ind,
  c.op_replenishment_eligible_ind,
  c.size_range_desc,
  c.size_sequence_num,
  c.size_range_code,
  c.eff_begin_tmstp_utc as eff_begin_tmstp,
  c.eff_begin_tmstp_tz,
  c.eff_end_tmstp_utc as eff_end_tmstp,
  c.eff_end_tmstp_tz,
   (SELECT batch_id
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  MIN(b.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_begin,
  MIN(b.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_end,
  MIN(c.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_begin,
  MIN(c.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_end,
  'N' AS process_flag
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS a
  INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(a.channel_country
      ) = LOWER(b.channel_country) AND a.epm_sku_num = b.epm_sku_num
  INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS c ON LOWER(b.rms_sku_num) = LOWER(c.rms_sku_num) AND LOWER(b.channel_country
      ) = LOWER(c.channel_country) AND b.epm_sku_num <> c.epm_sku_num
 WHERE  RANGE_OVERLAPS(RANGE(b.eff_begin_tmstp_utc,b.eff_end_tmstp_utc) ,RANGE(c.eff_begin_tmstp_utc,c.eff_end_tmstp_utc))
	  AND (a.rms_sku_num, a.channel_country) IN (SELECT (rms_sku_num, channel_country)
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
    GROUP BY rms_sku_num,
     channel_country
    HAVING COUNT(DISTINCT epm_sku_num) > 1)
    AND RANGE_CONTAINS(range(a.eff_begin_tmstp_utc,a.eff_end_tmstp_utc) , CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP)))
  WHERE eff_begin_tmstp >= new_epm_eff_begin);


DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS target
WHERE EXISTS (SELECT *
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg AS source
    WHERE LOWER(target.rms_sku_num) = LOWER(rms_sku_num) 
    AND LOWER(target.channel_country) = LOWER(channel_country) 
    AND target.epm_sku_num = old_epm_sku_num 
    AND target.eff_begin_tmstp >= new_epm_eff_begin_utc 
    AND LOWER(process_flag) = LOWER('N'));




UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS target 
SET
    eff_begin_tmstp = SOURCE.eff_end_tmstp_prev,
    eff_begin_tmstp_tz = `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(eff_end_tmstp_prev AS STRING)),
    dw_batch_id = (SELECT batch_id
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) 
        FROM (SELECT rms_sku_num, epm_sku_num, channel_country, eff_begin_tmstp_utc, eff_end_tmstp_utc, MIN(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp_utc ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS eff_end_tmstp_prev
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
        WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
                    FROM (SELECT DISTINCT rms_sku_num, channel_country
                            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg
                            WHERE LOWER(process_flag) = LOWER('N') AND product_sku_dim_hist.dw_batch_id = (SELECT batch_id
                                        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
                                        WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))) AS t4)
        QUALIFY eff_begin_tmstp_utc <> (MIN(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp_utc ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))) AS SOURCE
WHERE LOWER(SOURCE.rms_sku_num) = LOWER(target.rms_sku_num) 
AND LOWER(SOURCE.channel_country) = LOWER(target.channel_country) 
AND SOURCE.eff_begin_tmstp_utc = target.eff_begin_tmstp 
AND SOURCE.eff_end_tmstp_utc = target.eff_end_tmstp 
AND SOURCE.eff_end_tmstp_prev <> target.eff_begin_tmstp;

UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_overlap_stg SET
    process_flag = 'Y'
WHERE LOWER(process_flag) = LOWER('N') AND dw_batch_id = (SELECT batch_id
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'));

COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
END;

