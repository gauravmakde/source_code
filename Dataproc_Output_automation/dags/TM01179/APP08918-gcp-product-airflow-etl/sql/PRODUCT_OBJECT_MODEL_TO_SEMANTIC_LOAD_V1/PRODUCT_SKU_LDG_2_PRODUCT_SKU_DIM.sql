
TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw;





INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw (rms_sku_num, 
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
 eff_begin_tmstp_tz,
 eff_end_tmstp,
 eff_end_tmstp_tz)

with src_2 as
 (select legacyrmsskuid, id, marketcode, shortdescription, description, webskuid, brandlabelid, brandlabeldisplayname, legacyrmsstyleids_, parentepmstyleid, partnerrelationshipid, partnerrelationshiptype, webstyleid, parentepmchoiceid, sizes_edinrfsizecode, sizes_size1code, sizes_size1description, sizes_size2code, sizes_size2description, returndispositioncode, returndispositiondescription, sellingstatuscode, sellingstatusdescription, fulfillmenttypeeligibilities_fulfillmenttypecode, live_date, drop_ship_eligible_ind, prmy_supp_num, vendor_part_num, vendor_color, vendor_size, manufacturer_num, hazardousmaterialclassdescription, hazardousmaterialclasscode, sellingchanneleligibilities, issample, msrpamtplaintxt, msrpcurrcycd, vendorupcs_vendors_isnpgvendor, vendorupcs_vendors_orderquantitymultiple, isfpforecasteligible, isfpitemplanningeligible, isfpreplenishmenteligible, isopforecasteligible, isopitemplanningeligible, isopreplenishmenteligible, sizes_rangedescription, sizes_seqnum, sizes_rangecode,eff_begin_tmstp,eff_end_tmstp
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT legacyrmsskuid, id, marketcode, shortdescription, description, webskuid, brandlabelid, brandlabeldisplayname, legacyrmsstyleids_, parentepmstyleid, partnerrelationshipid, partnerrelationshiptype, webstyleid, parentepmchoiceid, sizes_edinrfsizecode, sizes_size1code, sizes_size1description, sizes_size2code, sizes_size2description, returndispositioncode, returndispositiondescription, sellingstatuscode, sellingstatusdescription, fulfillmenttypeeligibilities_fulfillmenttypecode, live_date, drop_ship_eligible_ind, prmy_supp_num, vendor_part_num, vendor_color, vendor_size, manufacturer_num, hazardousmaterialclassdescription, hazardousmaterialclasscode, sellingchanneleligibilities, issample, msrpamtplaintxt, msrpcurrcycd, vendorupcs_vendors_isnpgvendor, vendorupcs_vendors_orderquantitymultiple, isfpforecasteligible, isfpitemplanningeligible, isfpreplenishmenteligible, isopforecasteligible, isopitemplanningeligible, isopreplenishmenteligible, sizes_rangedescription, sizes_seqnum, sizes_rangecode, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY legacyrmsskuid, id, marketcode, shortdescription, description, webskuid, brandlabelid, brandlabeldisplayname, legacyrmsstyleids_, parentepmstyleid, partnerrelationshipid, partnerrelationshiptype, webstyleid, parentepmchoiceid, sizes_edinrfsizecode, sizes_size1code, sizes_size1description, sizes_size2code, sizes_size2description, returndispositioncode, returndispositiondescription, sellingstatuscode, sellingstatusdescription, fulfillmenttypeeligibilities_fulfillmenttypecode, live_date, drop_ship_eligible_ind, prmy_supp_num, vendor_part_num, vendor_color, vendor_size, manufacturer_num, hazardousmaterialclassdescription, hazardousmaterialclasscode, sellingchanneleligibilities, issample, msrpcurrcycd, vendorupcs_vendors_isnpgvendor, vendorupcs_vendors_orderquantitymultiple, isfpforecasteligible, isfpitemplanningeligible, isfpreplenishmenteligible, isopforecasteligible, isopitemplanningeligible, isopreplenishmenteligible, sizes_rangedescription, sizes_seqnum, sizes_rangecode ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY legacyrmsskuid, id, marketcode, shortdescription, description, webskuid, brandlabelid, brandlabeldisplayname, legacyrmsstyleids_, parentepmstyleid, partnerrelationshipid, partnerrelationshiptype, webstyleid, parentepmchoiceid, sizes_edinrfsizecode, sizes_size1code, sizes_size1description, sizes_size2code, sizes_size2description, returndispositioncode, returndispositiondescription, sellingstatuscode, sellingstatusdescription, fulfillmenttypeeligibilities_fulfillmenttypecode, live_date, drop_ship_eligible_ind, prmy_supp_num, vendor_part_num, vendor_color, vendor_size, manufacturer_num, hazardousmaterialclassdescription, hazardousmaterialclasscode, sellingchanneleligibilities, issample, msrpcurrcycd, vendorupcs_vendors_isnpgvendor, vendorupcs_vendors_orderquantitymultiple, isfpforecasteligible, isfpitemplanningeligible, isfpreplenishmenteligible, isopforecasteligible, isopitemplanningeligible, isopreplenishmenteligible, sizes_rangedescription, sizes_seqnum, sizes_rangecode ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from
				 (SELECT DISTINCT ldg.legacyrmsskuid,
           ldg.id,
           ldg.marketcode,
           ldg.shortdescription,
           ldg.description,
           ldg.webskuid,
           ldg.brandlabeldisplayname,
           ldg.brandlabelid,
           ldg.legacyrmsstyleids_,
           CAST(ldg.parentepmstyleid AS BIGINT) AS parentepmstyleid,
           ldg.partnerrelationshipid,
           ldg.partnerrelationshiptype,
           CAST(ldg.webstyleid AS BIGINT) AS webstyleid,
           CAST(ldg.parentepmchoiceid AS BIGINT) AS parentepmchoiceid,
           ldg.sizes_edinrfsizecode,
           ldg.sizes_size1code,
           ldg.sizes_size1description,
           ldg.sizes_size2code,
           ldg.sizes_size2description,
           ldg.returndispositioncode,
           ldg.returndispositiondescription,
            CASE
            WHEN selling_rights.rms_sku_num IS NULL
            THEN 'S1'
            ELSE selling_rights.selling_status_code
            END AS sellingstatuscode,
            CASE
            WHEN selling_rights.rms_sku_num IS NULL
            THEN 'Unsellable'
            ELSE selling_rights.selling_status_desc
            END AS sellingstatusdescription,
           ldg.fulfillmenttypeeligibilities_fulfillmenttypecode,
           selling_rights.live_date,
           ldg.hazardousmaterialclassdescription,
           ldg.hazardousmaterialclasscode,
            CASE
            WHEN selling_rights.rms_sku_num IS NULL
            THEN 'No Eligible Channels'
            ELSE selling_rights.selling_channel_eligibility_list
            END AS sellingchanneleligibilities,
           ldg.issample,
           ldg.msrpamtplaintxt,
           ldg.msrpcurrcycd,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_isdropshipeligible
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS drop_ship_eligible_ind,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_number
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prmy_supp_num,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_name
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prmy_supp_name,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_vendorproductnumbers_code
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vendor_part_num,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_vendorcolordescription
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vendor_color,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_vendorsizedescription
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vendor_size,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_manufacturer) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_number
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS manufacturer_num,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_isnpgvendor
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vendorupcs_vendors_isnpgvendor,
           MAX(CASE
             WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true'
                )
             THEN ldg.vendorupcs_vendors_orderquantitymultiple
             ELSE NULL
             END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN
            UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vendorupcs_vendors_orderquantitymultiple,
            CASE
            WHEN LOWER(ldg.isfpforecasteligible) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS isfpforecasteligible,
            CASE
            WHEN LOWER(ldg.isfpitemplanningeligible) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS isfpitemplanningeligible,
            CASE
            WHEN LOWER(ldg.isfpreplenishmenteligible) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS isfpreplenishmenteligible,
            CASE
            WHEN LOWER(ldg.isopforecasteligible) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS isopforecasteligible,
            CASE
            WHEN LOWER(ldg.isopitemplanningeligible) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS isopitemplanningeligible,
            CASE
            WHEN LOWER(ldg.isopreplenishmenteligible) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS isopreplenishmenteligible,
           ldg.sizes_rangedescription,
           ldg.sizes_seqnum,
           ldg.sizes_rangecode,
           cast(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp) as timestamp) as  eff_begin_tmstp,

            COALESCE(MAX(eff_begin_tmstp) OVER (PARTITION BY id, marketcode ORDER BY
              eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
          FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_ldg AS ldg
           LEFT JOIN (SELECT xref.rms_sku_num,
              xref.channel_country,
               CASE
               WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
               THEN 'S1'
               WHEN LOWER(LDG_XREF_0.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                  ), LOWER('4060'))
               THEN 'T1'
               WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
               THEN 'C3'
               ELSE NULL
               END AS derived_selling_status_code,
               CASE
               WHEN LOWER(CASE
                  WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_0.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
                  THEN 'C3'
                  ELSE NULL
                  END) = LOWER('S1')
               THEN 'Unsellable'
               WHEN LOWER(CASE
                  WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_0.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
                  THEN 'C3'
                  ELSE NULL
                  END) = LOWER('C3')
               THEN 'Sellable'
               WHEN LOWER(CASE
                  WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_0.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
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
               WHEN LOWER(LDG_XREF_0.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                  ), LOWER('4060'))
               THEN 'T1'
               WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
               THEN 'C3'
               ELSE NULL
               END AS selling_status_code,
              xref.live_date,
              xref.selling_channel_eligibility_list,
              xref.eff_begin_tmstp  AS eff_begin_tmstp,
              xref.eff_end_tmstp
             FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
              INNER JOIN (SELECT legacyrmsskuid AS rms_sku_num,
                marketcode AS channel_country,
                CAST(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp)as timestamp) AS source_publish_timestamp,
                fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
               FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_ldg
               WHERE legacyrmsskuid IS NOT NULL
               GROUP BY rms_sku_num,
                channel_country,
                source_publish_timestamp,
                fulfillment_type_code) AS LDG_XREF_0 ON LOWER(xref.rms_sku_num) = LOWER(LDG_XREF_0.rms_sku_num) AND
                 LOWER(xref.channel_country) = LOWER(LDG_XREF_0.channel_country) 
                  AND RANGE_CONTAINS(RANGE(XREF.eff_begin_tmstp,XREF.eff_end_tmstp) ,(LDG_XREF_0.source_publish_timestamp))

                 AND xref.selling_channel_eligibility_list
               IS NOT NULL
             UNION ALL
             SELECT xref0.rms_sku_num,
              xref0.channel_country,
               CASE
               WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
               THEN 'S1'
               WHEN LOWER(LDG_XREF_2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                  ), LOWER('4060'))
               THEN 'T1'
               WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
               THEN 'C3'
               ELSE NULL
               END AS derived_selling_status_code,
               CASE
               WHEN LOWER(CASE
                  WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
                  THEN 'C3'
                  ELSE NULL
                  END) = LOWER('S1')
               THEN 'Unsellable'
               WHEN LOWER(CASE
                  WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
                  THEN 'C3'
                  ELSE NULL
                  END) = LOWER('C3')
               THEN 'Sellable'
               WHEN LOWER(CASE
                  WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
                  THEN 'C3'
                  ELSE NULL
                  END) = LOWER('T1')
               THEN 'Dropship'
               ELSE NULL
               END AS selling_status_desc,
               CASE
               WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
               THEN 'S1'
               WHEN LOWER(LDG_XREF_2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                  ), LOWER('4060'))
               THEN 'T1'
               WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
               THEN 'C3'
               ELSE NULL
               END AS selling_status_code,
              xref0.live_date,
              xref0.selling_channel_eligibility_list,
              LDG_XREF_2.source_publish_timestamp AS eff_begin_tmstp,
              xref0.eff_end_tmstp
             FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref0
              INNER JOIN (SELECT LDG_XREF_1.rms_sku_num,
                LDG_XREF_1.channel_country,
                LDG_XREF_1.source_publish_timestamp,
                LDG_XREF_1.fulfillment_type_code
               FROM (SELECT legacyrmsskuid AS rms_sku_num,
                  marketcode AS channel_country,
                 CAST(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp)as timestamp) AS source_publish_timestamp,
                  fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
                 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_ldg
                 WHERE legacyrmsskuid IS NOT NULL
                 GROUP BY rms_sku_num,
                  channel_country,
                  source_publish_timestamp,
                  fulfillment_type_code) AS LDG_XREF_1
                LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1 ON LOWER(xref1.rms_sku_num) = LOWER(LDG_XREF_1
                    .rms_sku_num) AND LOWER(xref1.channel_country) = LOWER(LDG_XREF_1.channel_country) 
                          AND RANGE_CONTAINS(RANGE(XREF1.eff_begin_tmstp,XREF1.eff_end_tmstp) , (LDG_XREF_1.source_publish_timestamp)) 
                    AND xref1.selling_channel_eligibility_list
                 IS NOT NULL
               WHERE xref1.rms_sku_num IS NULL) AS LDG_XREF_2 ON LOWER(xref0.rms_sku_num) = LOWER(LDG_XREF_2.rms_sku_num
                    ) AND LOWER(xref0.channel_country) = LOWER(LDG_XREF_2.channel_country) AND xref0.eff_begin_tmstp >= CAST(LDG_XREF_2.source_publish_timestamp AS TIMESTAMP)
                   AND CAST(LDG_XREF_2.source_publish_timestamp AS TIMESTAMP) < xref0.eff_end_tmstp
                  AND xref0.selling_channel_eligibility_list IS NOT NULL
             QUALIFY (ROW_NUMBER() OVER (PARTITION BY xref0.rms_sku_num, xref0.channel_country ORDER BY LDG_XREF_2.source_publish_timestamp
                  )) = 1
             UNION ALL
             SELECT xref2.rms_sku_num,
              xref2.channel_country,
               CASE
               WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
               THEN 'S1'
               WHEN LOWER(LDG_XREF_3.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                  ), LOWER('4060'))
               THEN 'T1'
               ELSE 'C3'
               END AS derived_selling_status_code,
               CASE
               WHEN LOWER(CASE
                  WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_3.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  ELSE 'C3'
                  END) = LOWER('S1')
               THEN 'Unsellable'
               WHEN LOWER(CASE
                  WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_3.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  ELSE 'C3'
                  END) = LOWER('C3')
               THEN 'Sellable'
               WHEN LOWER(CASE
                  WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
                  THEN 'S1'
                  WHEN LOWER(LDG_XREF_3.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                     ), LOWER('4060'))
                  THEN 'T1'
                  ELSE 'C3'
                  END) = LOWER('T1')
               THEN 'Dropship'
               ELSE NULL
               END AS selling_status_desc,
               CASE 
               WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
               THEN 'S1'
               WHEN LOWER(LDG_XREF_3.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'
                  ), LOWER('4060'))
               THEN 'T1'
               ELSE 'C3'
               END AS selling_status_code,
              xref2.live_date,
              'No Eligible Channels' AS selling_channel_eligibility_list,
              xref2.eff_begin_tmstp  AS eff_begin_tmstp,
              CAST('9999-12-31 23:59:59.999999+00:00' AS timestamp) AS eff_end_tmstp 
             FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref2
              INNER JOIN (SELECT LDG_XREF_2.rms_sku_num,
                LDG_XREF_2.channel_country,
                source_publish_timestamp,
                LDG_XREF_2.fulfillment_type_code
               FROM (SELECT LDG_XREF_1.rms_sku_num,
                  LDG_XREF_1.channel_country,
                  LDG_XREF_1.source_publish_timestamp,
                  LDG_XREF_1.fulfillment_type_code
                 FROM (SELECT legacyrmsskuid AS rms_sku_num,
                    marketcode AS channel_country,
                    CAST(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp)as timestamp) AS source_publish_timestamp,
                    fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
                   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_ldg
                   WHERE legacyrmsskuid IS NOT NULL
                   GROUP BY rms_sku_num,
                    channel_country,
                    source_publish_timestamp,
                    fulfillment_type_code) AS LDG_XREF_1
                  LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref10 ON LOWER(xref10.rms_sku_num) = LOWER(LDG_XREF_1
                      .rms_sku_num) AND LOWER(xref10.channel_country) = LOWER(LDG_XREF_1.channel_country) AND xref10.selling_channel_eligibility_list
                   IS NOT NULL
                 WHERE xref10.rms_sku_num IS NULL) AS LDG_XREF_2
                LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref20 ON LOWER(xref20.rms_sku_num) = LOWER(LDG_XREF_2
                      .rms_sku_num) AND LOWER(xref20.channel_country) = LOWER(LDG_XREF_2.channel_country) AND xref20.eff_begin_tmstp
                     >= CAST(LDG_XREF_2.source_publish_timestamp AS TIMESTAMP) AND CAST(LDG_XREF_2.source_publish_timestamp AS TIMESTAMP)
                   < xref20.eff_end_tmstp AND xref20.selling_channel_eligibility_list IS NOT NULL
               WHERE xref20.rms_sku_num IS NULL) AS LDG_XREF_3 ON LOWER(xref2.rms_sku_num) = LOWER(LDG_XREF_3.rms_sku_num
                    ) AND LOWER(xref2.channel_country) = LOWER(LDG_XREF_3.channel_country) AND xref2.eff_begin_tmstp <= CAST(LDG_XREF_3.source_publish_timestamp AS TIMESTAMP)
                   AND CAST(LDG_XREF_3.source_publish_timestamp AS TIMESTAMP) >= xref2.eff_end_tmstp
                  AND xref2.selling_channel_eligibility_list IS NOT NULL
              LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref_4 ON LOWER(LDG_XREF_3.rms_sku_num) = LOWER(xref_4
                  .rms_sku_num) 
                  AND LOWER(LDG_XREF_3.channel_country) = LOWER(xref_4.channel_country) 
                  AND RANGE_CONTAINS(RANGE(XREF_4.eff_begin_tmstp,XREF_4.eff_end_tmstp) , (LDG_XREF_3.source_publish_timestamp)) 
                  AND xref_4.selling_channel_eligibility_list
               IS NULL
             QUALIFY (ROW_NUMBER() OVER (PARTITION BY xref2.rms_sku_num, xref2.channel_country ORDER BY LDG_XREF_3.source_publish_timestamp
                  DESC)) = 1) AS selling_rights 
                  ON LOWER(selling_rights.rms_sku_num) = LOWER(ldg.legacyrmsskuid) 
                  AND
             LOWER(selling_rights.channel_country) = LOWER(ldg.marketcode)
                AND RANGE_CONTAINS(RANGE(selling_rights.eff_begin_tmstp, selling_rights.eff_end_tmstp) , CAST(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp) as timestamp))
                QUALIFY eff_begin_tmstp < eff_end_tmstp
             )
) AS ordered_data
) AS grouped_data
GROUP BY legacyrmsskuid, id, marketcode, shortdescription, description, webskuid, brandlabelid, brandlabeldisplayname, legacyrmsstyleids_, parentepmstyleid, partnerrelationshipid, partnerrelationshiptype, webstyleid, parentepmchoiceid, sizes_edinrfsizecode, sizes_size1code, sizes_size1description, sizes_size2code, sizes_size2description, returndispositioncode, returndispositiondescription, sellingstatuscode, sellingstatusdescription, fulfillmenttypeeligibilities_fulfillmenttypecode, live_date, drop_ship_eligible_ind, prmy_supp_num, vendor_part_num, vendor_color, vendor_size, manufacturer_num, hazardousmaterialclassdescription, hazardousmaterialclasscode, sellingchanneleligibilities, issample, msrpamtplaintxt, msrpcurrcycd, vendorupcs_vendors_isnpgvendor, vendorupcs_vendors_orderquantitymultiple, isfpforecasteligible, isfpitemplanningeligible, isfpreplenishmenteligible, isopforecasteligible, isopitemplanningeligible, isopreplenishmenteligible, sizes_rangedescription, sizes_seqnum, sizes_rangecode,range_group
ORDER BY legacyrmsskuid, id, marketcode, shortdescription, description, webskuid, brandlabelid, brandlabeldisplayname, legacyrmsstyleids_, parentepmstyleid, partnerrelationshipid, partnerrelationshiptype, webstyleid, parentepmchoiceid, sizes_edinrfsizecode, sizes_size1code, sizes_size1description, sizes_size2code, sizes_size2description, returndispositioncode, returndispositiondescription, sellingstatuscode, sellingstatusdescription, fulfillmenttypeeligibilities_fulfillmenttypecode, live_date, drop_ship_eligible_ind, prmy_supp_num, vendor_part_num, vendor_color, vendor_size, manufacturer_num, hazardousmaterialclassdescription, hazardousmaterialclasscode, sellingchanneleligibilities, issample, msrpamtplaintxt, msrpcurrcycd, vendorupcs_vendors_isnpgvendor, vendorupcs_vendors_orderquantitymultiple, isfpforecasteligible, isfpitemplanningeligible, isfpreplenishmenteligible, isopforecasteligible, isopitemplanningeligible, isopreplenishmenteligible, sizes_rangedescription, sizes_seqnum, sizes_rangecode, eff_begin_tmstp) ),


 src_4 as
 (select rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, MIN(eff_begin_tmstp1) AS eff_begin_tmstp, MAX(eff_end_tmstp1) AS eff_end_tmstp,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ORDER BY eff_begin_tmstp1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT sku0.rms_sku_num,sku0.epm_sku_num,sku0.channel_country,sku0.sku_short_desc,sku0.sku_desc,sku0.web_sku_num,sku0.brand_label_num,sku0.brand_label_display_name,sku0.rms_style_num,sku0.epm_style_num,sku0.partner_relationship_num,sku0.partner_relationship_type_code,sku0.web_style_num,styl0.style_desc,sku0.epm_choice_num,sku0.supp_part_num,sku0.prmy_supp_num,sku0.manufacturer_num,styl0.sbclass_num,styl0.sbclass_desc,styl0.class_num,styl0.class_desc,styl0.dept_num,styl0.dept_desc,dept0.subdivision_num,dept0.subdivision_short_name,dept0.division_num,dept0.division_short_name,choice0.nrf_color_num,choice0.color_desc,choice0.nord_display_color,sku0.nrf_size_code,sku0.size_1_num,sku0.size_1_desc,sku0.size_2_num,sku0.size_2_desc,sku0.supp_color,sku0.supp_size,styl0.vendor_label_name,sku0.return_disposition_code,sku0.return_disposition_desc,sku0.selling_status_code,sku0.selling_status_desc,sku0.live_date,sku0.drop_ship_eligible_ind,sku0.sku_type_code,sku0.sku_type_desc,sku0.hazardous_material_class_desc,sku0.hazardous_material_class_code,sku0.fulfillment_type_code,sku0.selling_channel_eligibility_list,sku0.smart_sample_ind,sku0.gwp_ind,sku0.msrp_amt,sku0.msrp_currency_code,sku0.npg_ind,sku0.order_quantity_multiple,sku0.fp_forecast_eligible_ind,sku0.fp_item_planning_eligible_ind,sku0.fp_replenishment_eligible_ind,sku0.op_forecast_eligible_ind,sku0.op_item_planning_eligible_ind,sku0.op_replenishment_eligible_ind,sku0.size_range_desc,sku0.size_sequence_num,sku0.size_range_code,
            CASE 
                WHEN LAG(SKU0.eff_end_tmstp_utc) OVER (PARTITION BY sku0.rms_sku_num,sku0.epm_sku_num,sku0.channel_country,sku0.sku_short_desc,sku0.sku_desc,sku0.web_sku_num,sku0.brand_label_num,sku0.brand_label_display_name,sku0.rms_style_num,sku0.epm_style_num,sku0.partner_relationship_num,sku0.partner_relationship_type_code,sku0.web_style_num,styl0.style_desc,sku0.epm_choice_num,sku0.supp_part_num,sku0.prmy_supp_num,sku0.manufacturer_num,styl0.sbclass_num,styl0.sbclass_desc,styl0.class_num,styl0.class_desc,styl0.dept_num,styl0.dept_desc,dept0.subdivision_num,dept0.subdivision_short_name,dept0.division_num,dept0.division_short_name,choice0.nrf_color_num,choice0.color_desc,choice0.nord_display_color,sku0.nrf_size_code,sku0.size_1_num,sku0.size_1_desc,sku0.size_2_num,sku0.size_2_desc,sku0.supp_color,sku0.supp_size,styl0.vendor_label_name,sku0.return_disposition_code,sku0.return_disposition_desc,sku0.selling_status_code,sku0.selling_status_desc,sku0.live_date,sku0.drop_ship_eligible_ind,sku0.sku_type_code,sku0.sku_type_desc,sku0.hazardous_material_class_desc,sku0.hazardous_material_class_code,sku0.fulfillment_type_code,sku0.selling_channel_eligibility_list,sku0.smart_sample_ind,sku0.gwp_ind,sku0.msrp_amt,sku0.msrp_currency_code,sku0.npg_ind,sku0.order_quantity_multiple,sku0.fp_forecast_eligible_ind,sku0.fp_item_planning_eligible_ind,sku0.fp_replenishment_eligible_ind,sku0.op_forecast_eligible_ind,sku0.op_item_planning_eligible_ind,sku0.op_replenishment_eligible_ind,sku0.size_range_desc,sku0.size_sequence_num,sku0.size_range_code ORDER BY SKU0.eff_begin_tmstp_utc) >= 
                DATE_SUB(SKU0.eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
						SKU0.eff_end_tmstp_utc as eff_end_tmstp1,
						SKU0.eff_begin_tmstp_utc as eff_begin_tmstp1,
             COALESCE(SAFE.RANGE_INTERSECT(SAFE.RANGE_INTERSECT((CASE WHEN styl0.epm_style_num>0 THEN SAFE.RANGE_INTERSECT(range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) ,RANGE(styl0.eff_begin_tmstp_utc,styl0.eff_end_tmstp_utc)) ELSE range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) END)
     ,
	 (CASE WHEN dept0.dept_num>0 THEN SAFE.RANGE_INTERSECT(range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) , RANGE(dept0.eff_begin_tmstp_utc,dept0.eff_end_tmstp_utc)) ELSE range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) END))
     ,
	 CASE WHEN choice0.epm_choice_num>0 THEN SAFE.RANGE_INTERSECT(range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) , range(choice0.eff_begin_tmstp_utc,choice0.eff_end_tmstp_utc)) ELSE range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) END
     )
          , range(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) ) AS eff_period 

         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS sku0
       LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS styl0 
       ON sku0.epm_style_num = styl0.epm_style_num AND LOWER(sku0
          .channel_country) = LOWER(styl0.channel_country)
           AND RANGE_OVERLAPS(RANGE(sku0.eff_begin_tmstp_utc,sku0.eff_end_tmstp_utc) , RANGE(styl0.eff_begin_tmstp_utc, styl0.eff_end_tmstp_utc))
       LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS dept0 ON dept0.dept_num = CAST(TRIM(FORMAT('%11d', styl0.dept_num)) AS FLOAT64)
        AND RANGE_OVERLAPS(RANGE(sku0.eff_begin_tmstp_utc,sku0.eff_end_tmstp_utc)  , RANGE(dept0.eff_begin_tmstp_utc, dept0.eff_end_tmstp_utc))
        AND RANGE_OVERLAPS(RANGE(styl0.eff_begin_tmstp_utc, styl0.eff_end_tmstp_utc) , RANGE(dept0.eff_begin_tmstp_utc, dept0.eff_end_tmstp_utc))
        
       LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_choice_dim_hist AS choice0 ON sku0.epm_choice_num = choice0.epm_choice_num AND
         LOWER(sku0.channel_country) = LOWER(choice0.channel_country)
         AND RANGE_OVERLAPS(RANGE(sku0.eff_begin_tmstp_utc,sku0.eff_end_tmstp_utc),RANGE(choice0.eff_begin_tmstp_utc, choice0.eff_end_tmstp_utc))
        AND RANGE_OVERLAPS(RANGE(styl0.eff_begin_tmstp_utc, styl0.eff_end_tmstp_utc) , RANGE(choice0.eff_begin_tmstp_utc, choice0.eff_end_tmstp_utc))
        AND RANGE_OVERLAPS(RANGE(dept0.eff_begin_tmstp_utc, dept0.eff_end_tmstp_utc) , RANGE(choice0.eff_begin_tmstp_utc, choice0.eff_end_tmstp_utc))
      WHERE sku0.dw_sys_load_tmstp >= DATETIME_SUB(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
         ,INTERVAL 2 DAY)
       AND LOWER(sku0.sku_type_code) = LOWER('f')
       AND NOT EXISTS (SELECT 1
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_ldg AS ldg
        WHERE id = sku0.epm_sku_num
         AND LOWER(marketcode) = LOWER(sku0.channel_country))
				 ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,vendor_label_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, eff_begin_tmstp) ) ,

				



 src_3 as
 (select legacyrmsskuid,id,marketcode,shortdescription,description1 as description ,webskuid,brandlabelid,brandlabeldisplayname,legacyrmsstyleids_,parentepmstyleid,partnerrelationshipid,partnerrelationshiptype,webstyleid,style_desc,parentepmchoiceid,vendor_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,sizes_edinrfsizecode,sizes_size1code,sizes_size1description,sizes_size2code,sizes_size2description,vendor_color,vendor_size,vendor_label_name,returndispositioncode,returndispositiondescription,sellingstatuscode,sellingstatusdescription,live_date,drop_ship_eligible_ind,hazardousmaterialclassdescription,hazardousmaterialclasscode,fulfillmenttypeeligibilities_fulfillmenttypecode,sellingchanneleligibilities,issample,msrpamtplaintxt,msrpcurrcycd,vendorupcs_vendors_isnpgvendor,vendorupcs_vendors_orderquantitymultiple,isfpforecasteligible,isfpitemplanningeligible,isfpreplenishmenteligible,isopforecasteligible,isopitemplanningeligible,isopreplenishmenteligible,sizes_rangedescription,sizes_seqnum,sizes_rangecode,eff_begin_tmstp,eff_end_tmstp,eff_period
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT legacyrmsskuid,id,marketcode,shortdescription,description1,webskuid,brandlabelid,brandlabeldisplayname,legacyrmsstyleids_,parentepmstyleid,partnerrelationshipid,partnerrelationshiptype,webstyleid,style_desc,parentepmchoiceid,vendor_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,sizes_edinrfsizecode,sizes_size1code,sizes_size1description,sizes_size2code,sizes_size2description,vendor_color,vendor_size,vendor_label_name,returndispositioncode,returndispositiondescription,sellingstatuscode,sellingstatusdescription,live_date,drop_ship_eligible_ind,hazardousmaterialclassdescription,hazardousmaterialclasscode,fulfillmenttypeeligibilities_fulfillmenttypecode,sellingchanneleligibilities,issample,msrpamtplaintxt,msrpcurrcycd,vendorupcs_vendors_isnpgvendor,vendorupcs_vendors_orderquantitymultiple,isfpforecasteligible,isfpitemplanningeligible,isfpreplenishmenteligible,isopforecasteligible,isopitemplanningeligible,isopreplenishmenteligible,sizes_rangedescription,sizes_seqnum,sizes_rangecode, MIN(eff_begin_tmstp1) AS eff_begin_tmstp, MAX(eff_end_tmstp1) AS eff_end_tmstp,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY  legacyrmsskuid,id,marketcode,shortdescription,webskuid,brandlabelid,brandlabeldisplayname,legacyrmsstyleids_,parentepmstyleid,partnerrelationshipid,partnerrelationshiptype,webstyleid,style_desc,parentepmchoiceid,vendor_part_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,sizes_edinrfsizecode,sizes_size1code,sizes_size1description,sizes_size2code,sizes_size2description,vendor_color,vendor_size,vendor_label_name,returndispositioncode,returndispositiondescription,sellingstatuscode,sellingstatusdescription,live_date,drop_ship_eligible_ind,hazardousmaterialclassdescription,hazardousmaterialclasscode,fulfillmenttypeeligibilities_fulfillmenttypecode,sellingchanneleligibilities,issample,msrpcurrcycd,vendorupcs_vendors_isnpgvendor,vendorupcs_vendors_orderquantitymultiple,isfpforecasteligible,isfpitemplanningeligible,isfpreplenishmenteligible,isopforecasteligible,isopitemplanningeligible,isopreplenishmenteligible,sizes_rangedescription,sizes_seqnum,sizes_rangecode ORDER BY eff_begin_tmstp1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT SRC0.legacyrmsskuid,SRC0.id,SRC0.marketcode,SRC0.shortdescription,SRC0.description1,SRC0.webskuid,SRC0.brandlabelid,SRC0.brandlabeldisplayname,SRC0.legacyrmsstyleids_,SRC0.parentepmstyleid,SRC0.partnerrelationshipid,SRC0.partnerrelationshiptype,SRC0.webstyleid,styl.style_desc,SRC0.parentepmchoiceid,SRC0.vendor_part_num,SRC0.prmy_supp_num,SRC0.manufacturer_num,styl.sbclass_num,styl.sbclass_desc,styl.class_num,styl.class_desc,styl.dept_num,styl.dept_desc,dept.subdivision_num,dept.subdivision_short_name,dept.division_num,dept.division_short_name,choice.nrf_color_num,choice.color_desc,choice.nord_display_color,SRC0.sizes_edinrfsizecode,SRC0.sizes_size1code,SRC0.sizes_size1description,SRC0.sizes_size2code,SRC0.sizes_size2description,SRC0.vendor_color,SRC0.vendor_size,styl.vendor_label_name,SRC0.returndispositioncode,SRC0.returndispositiondescription,SRC0.sellingstatuscode,SRC0.sellingstatusdescription,SRC0.live_date,SRC0.drop_ship_eligible_ind,SRC0.hazardousmaterialclassdescription,SRC0.hazardousmaterialclasscode,SRC0.fulfillmenttypeeligibilities_fulfillmenttypecode,SRC0.sellingchanneleligibilities,SRC0.issample,SRC0.msrpcurrcycd,SRC0.vendorupcs_vendors_isnpgvendor,SRC0.vendorupcs_vendors_orderquantitymultiple,SRC0.isfpforecasteligible,SRC0.isfpitemplanningeligible,SRC0.isfpreplenishmenteligible,SRC0.isopforecasteligible,SRC0.isopitemplanningeligible,SRC0.isopreplenishmenteligible,SRC0.sizes_rangedescription,SRC0.sizes_seqnum,SRC0.sizes_rangecode,src0.msrpamtplaintxt,
            CASE 
                WHEN LAG(styl.eff_end_tmstp_utc) OVER (PARTITION BY SRC0.legacyrmsskuid,SRC0.id,SRC0.marketcode,SRC0.shortdescription,SRC0.description1,SRC0.webskuid,SRC0.brandlabelid,SRC0.brandlabeldisplayname,SRC0.legacyrmsstyleids_,SRC0.parentepmstyleid,SRC0.partnerrelationshipid,SRC0.partnerrelationshiptype,SRC0.webstyleid,styl.style_desc,SRC0.parentepmchoiceid,SRC0.vendor_part_num,SRC0.prmy_supp_num,SRC0.manufacturer_num,styl.sbclass_num,styl.sbclass_desc,styl.class_num,styl.class_desc,styl.dept_num,styl.dept_desc,dept.subdivision_num,dept.subdivision_short_name,dept.division_num,dept.division_short_name,choice.nrf_color_num,choice.color_desc,choice.nord_display_color,SRC0.sizes_edinrfsizecode,SRC0.sizes_size1code,SRC0.sizes_size1description,SRC0.sizes_size2code,SRC0.sizes_size2description,SRC0.vendor_color,SRC0.vendor_size,styl.vendor_label_name,SRC0.returndispositioncode,SRC0.returndispositiondescription,SRC0.sellingstatuscode,SRC0.sellingstatusdescription,SRC0.live_date,SRC0.drop_ship_eligible_ind,SRC0.hazardousmaterialclassdescription,SRC0.hazardousmaterialclasscode,SRC0.fulfillmenttypeeligibilities_fulfillmenttypecode,SRC0.sellingchanneleligibilities,SRC0.issample,SRC0.msrpcurrcycd,SRC0.vendorupcs_vendors_isnpgvendor,SRC0.vendorupcs_vendors_orderquantitymultiple,SRC0.isfpforecasteligible,SRC0.isfpitemplanningeligible,SRC0.isfpreplenishmenteligible,SRC0.isopforecasteligible,SRC0.isopitemplanningeligible,SRC0.isopreplenishmenteligible,SRC0.sizes_rangedescription,SRC0.sizes_seqnum,SRC0.sizes_rangecode ORDER BY styl.eff_begin_tmstp_utc) >= 
                DATE_SUB(styl.eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
              styl.eff_begin_tmstp_utc as eff_begin_tmstp1,
							styl.eff_end_tmstp_utc as eff_end_tmstp1,
            COALESCE(SAFE.RANGE_INTERSECT(SAFE.RANGE_INTERSECT((CASE WHEN styl.epm_style_num>0 THEN SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(styl.eff_begin_tmstp_utc,styl.eff_end_tmstp_utc)) ELSE SRC0.eff_period END),
		 (CASE WHEN dept.dept_num>0 THEN SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(dept.eff_begin_tmstp_utc,dept.eff_end_tmstp_utc)) ELSE SRC0.eff_period END)),
		 CASE WHEN choice.epm_choice_num>0 THEN SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(choice.eff_begin_tmstp_utc,choice.eff_end_tmstp_utc)) ELSE SRC0.eff_period END
	     ), SRC0.eff_period ) AS eff_period
         from
(SELECT 
       legacyrmsskuid,
         id,
         marketcode,
         shortdescription,
         description as description1 ,
         webskuid,
         brandlabelid,
         brandlabeldisplayname,
         legacyrmsstyleids_,
         parentepmstyleid,
         partnerrelationshipid,
          CASE
          WHEN partnerrelationshiptype IS NULL OR LOWER(TRIM(partnerrelationshiptype)) = LOWER('')
          THEN 'UNKNOWN'
          ELSE partnerrelationshiptype
          END AS partnerrelationshiptype,
         webstyleid,
         parentepmchoiceid,
         sizes_edinrfsizecode,
         sizes_size1code,
         sizes_size1description,
         sizes_size2code,
         sizes_size2description,
         returndispositioncode,
         returndispositiondescription,
         sellingstatuscode,
         sellingstatusdescription,
         fulfillmenttypeeligibilities_fulfillmenttypecode,
         live_date,
          CASE
          WHEN LOWER(drop_ship_eligible_ind) = LOWER('true')
          THEN 'Y'
          ELSE 'N'
          END AS drop_ship_eligible_ind,
         prmy_supp_num,
         vendor_part_num,
         vendor_color,
         vendor_size,
         manufacturer_num,
         hazardousmaterialclassdescription,
         hazardousmaterialclasscode,
         sellingchanneleligibilities,
         issample,
         msrpamtplaintxt,
         msrpcurrcycd,
          CASE
          WHEN LOWER(vendorupcs_vendors_isnpgvendor) = LOWER('true')
          THEN 'Y'
          ELSE 'N'
          END AS vendorupcs_vendors_isnpgvendor,
         CAST(vendorupcs_vendors_orderquantitymultiple AS INTEGER) AS vendorupcs_vendors_orderquantitymultiple,
         isfpforecasteligible,
         isfpitemplanningeligible,
         isfpreplenishmenteligible,
         isopforecasteligible,
         isopitemplanningeligible,
         isopreplenishmenteligible,
         sizes_rangedescription,
         sizes_seqnum,
         sizes_rangecode,
        RANGE(eff_begin_tmstp, eff_end_tmstp) AS  eff_period
        FROM  SRC_2
        QUALIFY eff_begin_tmstp < COALESCE(MAX(eff_begin_tmstp) OVER (PARTITION BY id, marketcode ORDER BY
              eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), '9999-12-31 23:59:59.999999+00:00')
            
						) AS SRC0
       LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS styl 
       ON SRC0.parentepmstyleid = styl.epm_style_num 
       AND LOWER(SRC0
          .marketcode) = LOWER(styl.channel_country)
         AND RANGE_OVERLAPS(src0.eff_period , RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc))
       LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS dept 
       ON dept.dept_num = CAST(TRIM(FORMAT('%11d', styl.dept_num)) AS FLOAT64)
       AND RANGE_OVERLAPS(src0.eff_period, RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc))
       AND RANGE_CONTAINS(RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc) , RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc))
        
       LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_choice_dim_hist AS choice 
       ON SRC0.parentepmchoiceid = choice.epm_choice_num
        AND LOWER(SRC0.marketcode) = LOWER(choice.channel_country)
        AND RANGE_OVERLAPS(src0.eff_period, RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc)) 
        AND RANGE_OVERLAPS(RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc), RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc))
        AND RANGE_OVERLAPS(RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc), RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc))
				 ) AS ordered_data
) AS grouped_data
GROUP BY legacyrmsskuid,id,marketcode,shortdescription,description1,webskuid,brandlabelid,brandlabeldisplayname,legacyrmsstyleids_,parentepmstyleid,partnerrelationshipid,partnerrelationshiptype,webstyleid,style_desc,parentepmchoiceid,vendor_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,sizes_edinrfsizecode,sizes_size1code,sizes_size1description,sizes_size2code,sizes_size2description,vendor_color,vendor_size,vendor_label_name,returndispositioncode,returndispositiondescription,sellingstatuscode,sellingstatusdescription,live_date,drop_ship_eligible_ind,hazardousmaterialclassdescription,hazardousmaterialclasscode,fulfillmenttypeeligibilities_fulfillmenttypecode,sellingchanneleligibilities,issample,msrpamtplaintxt,msrpcurrcycd,vendorupcs_vendors_isnpgvendor,vendorupcs_vendors_orderquantitymultiple,isfpforecasteligible,isfpitemplanningeligible,isfpreplenishmenteligible,isopforecasteligible,isopitemplanningeligible,isopreplenishmenteligible,sizes_rangedescription,sizes_seqnum,sizes_rangecode,range_group,eff_period
ORDER BY legacyrmsskuid,id,marketcode,shortdescription,description1,webskuid,brandlabelid,brandlabeldisplayname,legacyrmsstyleids_,parentepmstyleid,partnerrelationshipid,partnerrelationshiptype,webstyleid,style_desc,parentepmchoiceid,vendor_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,subdivision_num,subdivision_short_name,division_num,division_short_name,nrf_color_num,color_desc,nord_display_color,sizes_edinrfsizecode,sizes_size1code,sizes_size1description,sizes_size2code,sizes_size2description,vendor_color,vendor_size,vendor_label_name,returndispositioncode,returndispositiondescription,sellingstatuscode,sellingstatusdescription,live_date,drop_ship_eligible_ind,hazardousmaterialclassdescription,hazardousmaterialclasscode,fulfillmenttypeeligibilities_fulfillmenttypecode,sellingchanneleligibilities,issample,msrpamtplaintxt,msrpcurrcycd,vendorupcs_vendors_isnpgvendor,vendorupcs_vendors_orderquantitymultiple,isfpforecasteligible,isfpitemplanningeligible,isfpreplenishmenteligible,isopforecasteligible,isopitemplanningeligible,isopreplenishmenteligible,sizes_rangedescription,sizes_seqnum,sizes_rangecode, eff_begin_tmstp) ) 










SELECT rms_sku_num,
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
  ROUND(CAST(msrp_amt AS NUMERIC), 2) AS msrp_amt,
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
  size_range_code
, min(RANGE_START(eff_period)) AS eff_begin,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE` (CAST(min(RANGE_START(eff_period)) AS STRING)) AS eff_begin_tmstp_tz,
  max(RANGE_END(eff_period)) AS eff_end,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE` (CAST(max(RANGE_END(eff_period)) AS STRING)) AS eff_end_tmstp_tz
from  (
    --inner normalize
            SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT  SRC.rms_sku_num,SRC.epm_sku_num,SRC.channel_country,SRC.sku_short_desc,SRC.sku_desc,SRC.web_sku_num,SRC.brand_label_num,SRC.brand_label_display_name,SRC.rms_style_num,SRC.epm_style_num,SRC.partner_relationship_num,SRC.partner_relationship_type_code,SRC.web_style_num,SRC.style_desc,SRC.epm_choice_num,SRC.supp_part_num,SRC.prmy_supp_num,SRC.manufacturer_num,SRC.sbclass_num,SRC.sbclass_desc,SRC.class_num,SRC.class_desc,SRC.dept_num,SRC.dept_desc,SRC.grp_num,SRC.grp_desc,SRC.div_num,SRC.div_desc,SRC.cmpy_num,SRC.cmpy_desc,SRC.color_num,SRC.color_desc,SRC.nord_display_color,SRC.nrf_size_code,SRC.size_1_num,SRC.size_1_desc,SRC.size_2_num,SRC.size_2_desc,SRC.supp_color,SRC.supp_size,SRC.brand_name,SRC.return_disposition_code,SRC.return_disposition_desc,SRC.selling_status_code,SRC.selling_status_desc,SRC.live_date,SRC.drop_ship_eligible_ind,SRC.sku_type_code,SRC.sku_type_desc,SRC.hazardous_material_class_desc,SRC.hazardous_material_class_code,SRC.fulfillment_type_code,SRC.selling_channel_eligibility_list,SRC.smart_sample_ind,SRC.gwp_ind,SRC.msrp_amt,SRC.msrp_currency_code,SRC.npg_ind,SRC.order_quantity_multiple,SRC.fp_forecast_eligible_ind,SRC.fp_item_planning_eligible_ind,SRC.fp_replenishment_eligible_ind,SRC.op_forecast_eligible_ind,SRC.op_item_planning_eligible_ind,SRC.op_replenishment_eligible_ind,SRC.size_range_desc,SRC.size_sequence_num,SRC.size_range_code, TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY SRC.rms_sku_num,SRC.epm_sku_num,SRC.channel_country,SRC.sku_short_desc,SRC.sku_desc,SRC.web_sku_num,SRC.brand_label_num,SRC.brand_label_display_name,SRC.rms_style_num,SRC.epm_style_num,SRC.partner_relationship_num,SRC.partner_relationship_type_code,SRC.web_style_num,SRC.style_desc,SRC.epm_choice_num,SRC.supp_part_num,SRC.prmy_supp_num,SRC.manufacturer_num,SRC.sbclass_num,SRC.sbclass_desc,SRC.class_num,SRC.class_desc,SRC.dept_num,SRC.dept_desc,SRC.grp_num,SRC.grp_desc,SRC.div_num,SRC.div_desc,SRC.cmpy_num,SRC.cmpy_desc,SRC.color_num,SRC.color_desc,SRC.nord_display_color,SRC.nrf_size_code,SRC.size_1_num,SRC.size_1_desc,SRC.size_2_num,SRC.size_2_desc,SRC.supp_color,SRC.supp_size,SRC.brand_name,SRC.return_disposition_code,SRC.return_disposition_desc,SRC.selling_status_code,SRC.selling_status_desc,SRC.live_date,SRC.drop_ship_eligible_ind,SRC.sku_type_code,SRC.sku_type_desc,SRC.hazardous_material_class_desc,SRC.hazardous_material_class_code,SRC.fulfillment_type_code,SRC.selling_channel_eligibility_list,SRC.smart_sample_ind,SRC.gwp_ind,SRC.msrp_currency_code,SRC.npg_ind,SRC.order_quantity_multiple,SRC.fp_forecast_eligible_ind,SRC.fp_item_planning_eligible_ind,SRC.fp_replenishment_eligible_ind,SRC.op_forecast_eligible_ind,SRC.op_item_planning_eligible_ind,SRC.op_replenishment_eligible_ind,SRC.size_range_desc,SRC.size_sequence_num,SRC.size_range_code ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE(SAFE.RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period,
             FROM
 (SELECT 
   --normalize
           legacyrmsskuid AS rms_sku_num,
       id AS epm_sku_num,
       marketcode AS channel_country,
       shortdescription AS sku_short_desc,
       description AS sku_desc,
       webskuid AS web_sku_num,
       brandlabelid AS brand_label_num,
       brandlabeldisplayname AS brand_label_display_name,
       legacyrmsstyleids_ AS rms_style_num,
       parentepmstyleid AS epm_style_num,
       partnerrelationshipid AS partner_relationship_num,
       partnerrelationshiptype AS partner_relationship_type_code,
       webstyleid AS web_style_num,
       style_desc,
       parentepmchoiceid AS epm_choice_num,
       vendor_part_num AS supp_part_num,
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
       sizes_edinrfsizecode AS nrf_size_code,
       sizes_size1code AS size_1_num,
       sizes_size1description AS size_1_desc,
       sizes_size2code AS size_2_num,
       sizes_size2description AS size_2_desc,
       vendor_color AS supp_color,
       vendor_size AS supp_size,
       vendor_label_name AS brand_name,
       returndispositioncode AS return_disposition_code,
       returndispositiondescription AS return_disposition_desc,
       sellingstatuscode AS selling_status_code,
       sellingstatusdescription AS selling_status_desc,
       live_date,
       drop_ship_eligible_ind,
       'f' AS sku_type_code,
       'Fashion Sku' AS sku_type_desc,
       hazardousmaterialclassdescription AS hazardous_material_class_desc,
       hazardousmaterialclasscode AS hazardous_material_class_code,
       fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code,
       sellingchanneleligibilities AS selling_channel_eligibility_list,
        CASE
        WHEN LOWER(issample) = LOWER('true')
        THEN 'Y'
        ELSE 'N'
        END AS smart_sample_ind,
       COALESCE(CASE
         WHEN LOWER(description) LIKE LOWER('%GWP%') AND LOWER(class_desc) IN (LOWER('NO COST'), LOWER('CTS/BOL'
             ))
         THEN 'Y'
         ELSE 'N'
         END, 'N') AS gwp_ind,
       msrpamtplaintxt AS msrp_amt,
       msrpcurrcycd AS msrp_currency_code,
       vendorupcs_vendors_isnpgvendor AS npg_ind,
       vendorupcs_vendors_orderquantitymultiple AS order_quantity_multiple,
       isfpforecasteligible AS fp_forecast_eligible_ind,
       isfpitemplanningeligible AS fp_item_planning_eligible_ind,
       isfpreplenishmenteligible AS fp_replenishment_eligible_ind,
       isopforecasteligible AS op_forecast_eligible_ind,
       isopitemplanningeligible AS op_item_planning_eligible_ind,
       isopreplenishmenteligible AS op_replenishment_eligible_ind,
       sizes_rangedescription AS size_range_desc,
       sizes_seqnum AS size_sequence_num,
       sizes_rangecode AS size_range_code,
       eff_period
       FROM src_3
			 union all
			 select
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
			 eff_period from src_4) AS SRC
    LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS tgt 
    ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(SRC
       .channel_country) = LOWER(tgt.channel_country)
       AND RANGE_OVERLAPS(SRC.eff_period, RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))
   WHERE (tgt.rms_sku_num IS NULL
    OR ((SRC.epm_sku_num <> tgt.epm_sku_num
    OR (tgt.epm_sku_num IS NULL AND SRC.epm_sku_num IS NOT NULL) OR (SRC.epm_sku_num IS NULL AND tgt.epm_sku_num
         IS NOT NULL ))
         
         OR (LOWER(SRC.sku_short_desc) <> LOWER(tgt.sku_short_desc)
    OR (tgt.sku_short_desc IS NULL AND SRC.sku_short_desc IS NOT NULL) OR (SRC.sku_short_desc IS NULL AND tgt.sku_short_desc
          IS NOT NULL))
          
          OR (LOWER(SRC.sku_desc) <> LOWER(tgt.sku_desc) OR (tgt.sku_desc IS NULL AND SRC.sku_desc
         IS NOT NULL) OR (SRC.sku_desc IS NULL AND tgt.sku_desc IS NOT NULL)) 
         
         OR (SRC.web_sku_num <> tgt.web_sku_num
    OR (tgt.web_sku_num IS NULL AND SRC.web_sku_num IS NOT NULL) OR (SRC.web_sku_num IS NULL AND tgt.web_sku_num
           IS NOT NULL)) 
           
           OR (LOWER(SRC.brand_label_num) <> LOWER(tgt.brand_label_num) 
           OR (tgt.brand_label_num IS NULL AND
          SRC.brand_label_num IS NOT NULL) OR (SRC.brand_label_num IS NULL AND tgt.brand_label_num IS NOT NULL)) 
          
          OR (LOWER(SRC
             .brand_label_display_name) <> LOWER(tgt.brand_label_display_name) 
             OR (tgt.brand_label_display_name
          IS NULL AND SRC.brand_label_display_name IS NOT NULL) OR (SRC.brand_label_display_name IS NULL AND tgt.brand_label_display_name
            IS NOT NULL ))
            
            OR (LOWER(SRC.rms_style_num) <> LOWER(tgt.rms_style_num) OR (tgt.rms_style_num IS NULL AND SRC.rms_style_num
           IS NOT NULL) OR (SRC.rms_style_num IS NULL AND tgt.rms_style_num IS NOT NULL)) 
           
           
           
           OR (SRC.epm_style_num <> tgt.epm_style_num
    OR (tgt.epm_style_num IS NULL AND SRC.epm_style_num IS NOT NULL) OR (SRC.epm_style_num IS NULL AND tgt.epm_style_num
            IS NOT NULL)) 
            
            
            OR (LOWER(SRC.partner_relationship_num) <> LOWER(tgt.partner_relationship_num) 
            OR (tgt.partner_relationship_num
           IS NULL AND SRC.partner_relationship_num IS NOT NULL) OR (SRC.partner_relationship_num IS NULL AND tgt.partner_relationship_num
             IS NOT NULL)) 
             
             OR (LOWER(SRC.partner_relationship_type_code) <> LOWER(tgt.partner_relationship_type_code) OR
        (tgt.partner_relationship_type_code IS NULL AND SRC.partner_relationship_type_code IS NOT NULL) OR (SRC.partner_relationship_type_code
             IS NULL AND tgt.partner_relationship_type_code IS NOT NULL)) 
             
             OR (SRC.web_style_num <> tgt.web_style_num OR (tgt
            .web_style_num IS NULL AND SRC.web_style_num IS NOT NULL) OR (SRC.web_style_num IS NULL AND tgt.web_style_num
              IS NOT NULL)) 
              
              OR (LOWER(SRC.style_desc) <> LOWER(tgt.style_desc) OR (tgt.style_desc IS NULL AND SRC.style_desc
           IS NOT NULL) OR (SRC.style_desc IS NULL AND tgt.style_desc IS NOT NULL)) 
           
           OR (SRC.epm_choice_num <> tgt.epm_choice_num
         OR (tgt.epm_choice_num IS NULL AND SRC.epm_choice_num IS NOT NULL) OR (SRC.epm_choice_num IS NULL AND tgt
              .epm_choice_num IS NOT NULL ))
              
              OR (LOWER(SRC.supp_part_num) <> LOWER(tgt.supp_part_num) OR (tgt.supp_part_num
            IS NULL AND SRC.supp_part_num IS NOT NULL) OR (SRC.supp_part_num IS NULL AND tgt.supp_part_num IS NOT NULL)) 
            
            OR (LOWER(SRC.prmy_supp_num) <> LOWER(tgt.prmy_supp_num) OR (tgt.prmy_supp_num IS NULL AND SRC.prmy_supp_num
             IS NOT NULL) OR (SRC.prmy_supp_num IS NULL AND tgt.prmy_supp_num IS NOT NULL)) 
             
             OR (LOWER(SRC.manufacturer_num)
               <> LOWER(tgt.manufacturer_num)
    OR (tgt.manufacturer_num IS NULL AND SRC.manufacturer_num IS NOT NULL) OR (SRC.manufacturer_num IS NULL AND tgt.manufacturer_num
             IS NOT NULL)) 
             
             
             OR (SRC.sbclass_num <> tgt.sbclass_num OR (tgt.sbclass_num IS NULL AND SRC.sbclass_num
            IS NOT NULL) OR (SRC.sbclass_num IS NULL AND tgt.sbclass_num IS NOT NULL)) 
            
            OR (LOWER(SRC.sbclass_desc) <> LOWER(tgt
               .sbclass_desc) OR (tgt.sbclass_desc IS NULL AND SRC.sbclass_desc IS NOT NULL) OR (SRC.sbclass_desc
              IS NULL AND tgt.sbclass_desc IS NOT NULL)) 
              
              OR (SRC.class_num <> tgt.class_num OR (tgt.class_num IS NULL AND
             SRC.class_num IS NOT NULL) OR (SRC.class_num IS NULL AND tgt.class_num IS NOT NULL)) 
             
             OR (LOWER(SRC.class_desc)
               <> LOWER(tgt.class_desc) OR (tgt.class_desc IS NULL AND SRC.class_desc IS NOT NULL) OR (SRC.class_desc
              IS NULL AND tgt.class_desc IS NOT NULL)) 
              
              OR (SRC.dept_num <> tgt.dept_num OR (tgt.dept_num IS NULL AND SRC.dept_num
             IS NOT NULL) OR (SRC.dept_num IS NULL AND tgt.dept_num IS NOT NULL)) 
             
             OR (LOWER(SRC.dept_desc) <> LOWER(tgt.dept_desc
                ) OR (tgt.dept_desc IS NULL AND SRC.dept_desc IS NOT NULL) OR (SRC.dept_desc IS NULL AND tgt.dept_desc
               IS NOT NULL)) 
               
               OR (SRC.grp_num <> tgt.grp_num OR (tgt.grp_num IS NULL AND SRC.grp_num IS NOT NULL) OR (SRC.grp_num
                IS NULL AND tgt.grp_num IS NOT NULL)) 
                
                OR (LOWER(SRC.grp_desc) <> LOWER(tgt.grp_desc) OR (tgt.grp_desc
            IS NULL AND SRC.grp_desc IS NOT NULL) OR (SRC.grp_desc IS NULL AND tgt.grp_desc IS NOT NULL)) 
            
            OR (SRC.div_num <>
              tgt.div_num OR (tgt.div_num IS NULL AND SRC.div_num IS NOT NULL) OR (SRC.div_num IS NULL AND tgt.div_num
               IS NOT NULL)) 
               
               OR (LOWER(SRC.div_desc) <> LOWER(tgt.div_desc) OR (tgt.div_desc IS NULL AND SRC.div_desc
             IS NOT NULL) OR (SRC.div_desc IS NULL AND tgt.div_desc IS NOT NULL)) 
             
            OR (SRC.cmpy_num <> CAST(TGT.cmpy_num AS STRING) OR (SRC.cmpy_num IS NOT NULL AND TGT.cmpy_num IS NULL) OR (SRC.cmpy_num IS NULL AND TGT.cmpy_num IS NOT NULL))
               
               
               OR (SRC.cmpy_desc <> TGT.cmpy_desc OR (SRC.cmpy_desc IS NOT NULL AND TGT.cmpy_desc IS NULL) OR (SRC.cmpy_desc IS NULL AND TGT.cmpy_desc IS NOT NULL ))
        
        
        
        OR (LOWER(SRC.color_num) <> LOWER(tgt.color_num) OR (tgt.color_num IS NULL AND SRC.color_num IS NOT NULL) OR (SRC
               .color_num IS NULL AND tgt.color_num IS NOT NULL)) 
               
               
               OR (LOWER(SRC.color_desc) <> LOWER(tgt.color_desc) OR (tgt
                .color_desc IS NULL AND SRC.color_desc IS NOT NULL) OR (SRC.color_desc IS NULL AND tgt.color_desc
                IS NOT NULL)) 
                
                OR (LOWER(SRC.nord_display_color) <> LOWER(tgt.nord_display_color) OR (tgt.nord_display_color
                IS NULL AND SRC.nord_display_color IS NOT NULL) OR (SRC.nord_display_color IS NULL AND tgt.nord_display_color
                IS NOT NULL)) 
                
                OR (LOWER(SRC.nrf_size_code) <> LOWER(tgt.nrf_size_code) OR (tgt.nrf_size_code IS NULL AND
                 SRC.nrf_size_code IS NOT NULL) OR (SRC.nrf_size_code IS NULL AND tgt.nrf_size_code IS NOT NULL))
    
    
               OR (LOWER(SRC.size_1_num) <> LOWER(tgt.size_1_num) OR (tgt.size_1_num IS NULL AND SRC.size_1_num IS NOT NULL) OR (SRC
              .size_1_num IS NULL AND tgt.size_1_num IS NOT NULL)) 
              
              OR (LOWER(SRC.size_1_desc) <> LOWER(tgt.size_1_desc)
            OR (tgt.size_1_desc IS NULL AND SRC.size_1_desc IS NOT NULL) OR (SRC.size_1_desc IS NULL AND tgt.size_1_desc
               IS NOT NULL)) 
               
               OR (LOWER(SRC.size_2_num) <> LOWER(tgt.size_2_num) OR (tgt.size_2_num IS NULL AND SRC.size_2_num
               IS NOT NULL) OR (SRC.size_2_num IS NULL AND tgt.size_2_num IS NOT NULL)) 
               
               OR (LOWER(SRC.size_2_desc) <> LOWER(tgt
               .size_2_desc) OR (tgt.size_2_desc IS NULL AND SRC.size_2_desc IS NOT NULL) OR (SRC.size_2_desc IS NULL AND
                tgt.size_2_desc IS NOT NULL)) 
                
                OR (LOWER(SRC.supp_color) <> LOWER(tgt.supp_color) OR (tgt.supp_color
               IS NULL AND SRC.supp_color IS NOT NULL) OR (SRC.supp_color IS NULL AND tgt.supp_color IS NOT NULL)) 
               
               OR (LOWER(SRC
               .supp_size) <> LOWER(tgt.supp_size) OR (tgt.supp_size IS NULL AND SRC.supp_size IS NOT NULL) OR (SRC.supp_size
                IS NULL AND tgt.supp_size IS NOT NULL)) 
                
                OR (LOWER(SRC.brand_name) <> LOWER(tgt.brand_name) OR (tgt.brand_name
                IS NULL AND SRC.brand_name IS NOT NULL) OR (SRC.brand_name IS NULL AND tgt.brand_name IS NOT NULL)) 
                
                OR (LOWER(SRC
                .return_disposition_code) <> LOWER(tgt.return_disposition_code) OR (tgt.return_disposition_code IS NULL
                 AND SRC.return_disposition_code IS NOT NULL) OR (SRC.return_disposition_code IS NULL AND tgt.return_disposition_code
                 IS NOT NULL)) 
                 
                 
                 OR (LOWER(SRC.selling_status_code) <> LOWER(tgt.selling_status_code) OR (tgt.selling_status_code
               IS NULL AND SRC.selling_status_code IS NOT NULL) OR (SRC.selling_status_code IS NULL AND tgt.selling_status_code
               IS NOT NULL)) 
               
               
               OR (SRC.live_date <> tgt.live_date OR (tgt.live_date IS NULL AND SRC.live_date IS NOT NULL
               OR SRC.live_date IS NULL AND tgt.live_date IS NOT NULL)) 
               
               
               OR (LOWER(SRC.drop_ship_eligible_ind) <> LOWER(tgt
               .drop_ship_eligible_ind) OR (tgt.drop_ship_eligible_ind IS NULL AND SRC.drop_ship_eligible_ind
                IS NOT NULL )OR (SRC.drop_ship_eligible_ind IS NULL AND tgt.drop_ship_eligible_ind IS NOT NULL) )
                
                
                OR (LOWER(SRC
                .sku_type_code) <> LOWER(tgt.sku_type_code) OR (tgt.sku_type_code IS NULL AND SRC.sku_type_code
                 IS NOT NULL) OR (SRC.sku_type_code IS NULL AND tgt.sku_type_code IS NOT NULL))
                 
                 
                 OR (LOWER(SRC.sku_type_desc
               ) <> LOWER(tgt.sku_type_desc) OR (tgt.sku_type_desc IS NULL AND SRC.sku_type_desc IS NOT NULL) OR (SRC.sku_type_desc
                IS NULL AND tgt.sku_type_desc IS NOT NULL)) 
                
                
                OR (LOWER(SRC.hazardous_material_class_desc) <> LOWER(tgt.hazardous_material_class_desc
                ) OR (tgt.hazardous_material_class_desc IS NULL AND SRC.hazardous_material_class_desc IS NOT NULL) OR (SRC
                 .hazardous_material_class_desc IS NULL AND tgt.hazardous_material_class_desc IS NOT NULL)) 
                 
                 
                 OR (LOWER(SRC
                .hazardous_material_class_code) <> LOWER(tgt.hazardous_material_class_code) OR (tgt.hazardous_material_class_code
                 IS NULL AND SRC.hazardous_material_class_code IS NOT NULL) OR (SRC.hazardous_material_class_code IS NULL
                 AND tgt.hazardous_material_class_code IS NOT NULL)) 
                 
                 
                 OR (LOWER(SRC.fulfillment_type_code) <> LOWER(tgt.fulfillment_type_code
                 ) OR (tgt.fulfillment_type_code IS NULL AND SRC.fulfillment_type_code IS NOT NULL) OR (SRC.fulfillment_type_code
                  IS NULL AND tgt.fulfillment_type_code IS NOT NULL))
                  
                  
                  OR (LOWER(SRC.selling_channel_eligibility_list
              ) <> LOWER(tgt.selling_channel_eligibility_list) OR (tgt.selling_channel_eligibility_list IS NULL AND SRC
               .selling_channel_eligibility_list IS NOT NULL) OR (SRC.selling_channel_eligibility_list IS NULL AND tgt.selling_channel_eligibility_list
               IS NOT NULL)) 
               
               
               OR (LOWER(SRC.smart_sample_ind) <> LOWER(tgt.smart_sample_ind) OR (tgt.smart_sample_ind
                IS NULL AND SRC.smart_sample_ind IS NOT NULL) OR (SRC.smart_sample_ind IS NULL AND tgt.smart_sample_ind
                IS NOT NULL)) 
                
                
                OR (LOWER(SRC.gwp_ind) <> LOWER(tgt.gwp_ind) OR (tgt.gwp_ind IS NULL AND SRC.gwp_ind
                IS NOT NULL) OR (SRC.gwp_ind IS NULL AND tgt.gwp_ind IS NOT NULL)) 
                
                
                OR (SRC.msrp_amt <> tgt.msrp_amt OR (tgt
                 .msrp_amt IS NULL AND SRC.msrp_amt IS NOT NULL) OR (SRC.msrp_amt IS NULL AND tgt.msrp_amt IS NOT NULL))
         
         
         
         OR (LOWER(SRC.msrp_currency_code) <> LOWER(tgt.msrp_currency_code) OR (tgt.msrp_currency_code IS NULL AND SRC.msrp_currency_code
                IS NOT NULL) OR (SRC.msrp_currency_code IS NULL AND tgt.msrp_currency_code IS NOT NULL)) 
                
                
                
                OR (LOWER(SRC.npg_ind
                ) <> LOWER(tgt.npg_ind) OR (tgt.npg_ind IS NULL AND SRC.npg_ind IS NOT NULL) OR (SRC.npg_ind IS NULL AND
                 tgt.npg_ind IS NOT NULL)) 
                 
                 
                 OR (SRC.order_quantity_multiple <> tgt.order_quantity_multiple OR (tgt.order_quantity_multiple
                 IS NULL AND SRC.order_quantity_multiple IS NOT NULL) OR (SRC.order_quantity_multiple IS NULL AND tgt.order_quantity_multiple
                 IS NOT NULL)) 
                 
                 
                 
                 OR (LOWER(SRC.fp_forecast_eligible_ind) <> LOWER(tgt.fp_forecast_eligible_ind) OR (tgt.fp_forecast_eligible_ind
                  IS NULL AND SRC.fp_forecast_eligible_ind IS NOT NULL) OR (SRC.fp_forecast_eligible_ind IS NULL AND tgt.fp_forecast_eligible_ind
                  IS NOT NULL))
                  
                  
                  OR (LOWER(SRC.fp_item_planning_eligible_ind) <> LOWER(tgt.fp_item_planning_eligible_ind
               ) OR (tgt.fp_item_planning_eligible_ind IS NULL AND SRC.fp_item_planning_eligible_ind IS NOT NULL) OR (SRC
                .fp_item_planning_eligible_ind IS NULL AND tgt.fp_item_planning_eligible_ind IS NOT NULL)) 
                
                
                
                OR (LOWER(SRC
                .fp_replenishment_eligible_ind) <> LOWER(tgt.fp_replenishment_eligible_ind) OR (tgt.fp_replenishment_eligible_ind
                 IS NULL AND SRC.fp_replenishment_eligible_ind IS NOT NULL) OR (SRC.fp_replenishment_eligible_ind IS NULL
                 AND tgt.fp_replenishment_eligible_ind IS NOT NULL)) 
                 
                 
                 OR (LOWER(SRC.op_forecast_eligible_ind) <> LOWER(tgt
                .op_forecast_eligible_ind) OR (tgt.op_forecast_eligible_ind IS NULL AND SRC.op_forecast_eligible_ind
                 IS NOT NULL) OR (SRC.op_forecast_eligible_ind IS NULL AND tgt.op_forecast_eligible_ind IS NOT NULL)) 
                 
                 
                 OR (LOWER(SRC
                 .op_item_planning_eligible_ind) <> LOWER(tgt.op_item_planning_eligible_ind) OR (tgt.op_item_planning_eligible_ind
                  IS NULL AND SRC.op_item_planning_eligible_ind IS NOT NULL) OR (SRC.op_item_planning_eligible_ind IS NULL
                  AND tgt.op_item_planning_eligible_ind IS NOT NULL)) 
                  
                  
                  OR (LOWER(SRC.op_replenishment_eligible_ind) <>
               LOWER(tgt.op_replenishment_eligible_ind) OR (tgt.op_replenishment_eligible_ind IS NULL AND SRC.op_replenishment_eligible_ind
                 IS NOT NULL) OR (SRC.op_replenishment_eligible_ind IS NULL AND tgt.op_replenishment_eligible_ind
                 IS NOT NULL)) 
                 
                 
                 OR (LOWER(SRC.size_range_desc) <> LOWER(tgt.size_range_desc) OR (tgt.size_range_desc
                  IS NULL AND SRC.size_range_desc IS NOT NULL) OR (SRC.size_range_desc IS NULL AND tgt.size_range_desc
                  IS NOT NULL)) 
                  
                  
                  OR (LOWER(SRC.size_sequence_num) <> LOWER(tgt.size_sequence_num) OR (tgt.size_sequence_num
                  IS NULL AND SRC.size_sequence_num IS NOT NULL) OR (SRC.size_sequence_num IS NULL AND tgt.size_sequence_num
                  IS NOT NULL)) 
                  
                  
                  OR (LOWER(SRC.size_range_code) <> LOWER(tgt.size_range_code) OR (tgt.size_range_code
                   IS NULL AND SRC.size_range_code IS NOT NULL) OR (SRC.size_range_code IS NULL AND tgt.size_range_code
                   IS NOT NULL))
    ))

                    )as   ordered_data
) AS grouped_data)
GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_period
ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code;




BEGIN
BEGIN TRANSACTION;



-- SELECT * FROM {{params.dbenv}}_nap_base_vws.product_sku_ldg
--SEQUENCED VALIDTIME
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
    WHERE src.epm_sku_num = tgt.epm_sku_num 
    AND LOWER(channel_country) = LOWER(tgt.channel_country)
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp,
TGT.eff_end_tmstp_tz = SRC.eff_begin_tmstp_tz
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
  WHERE  src.epm_sku_num = tgt.epm_sku_num and
   LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND SRC.eff_begin_tmstp > tgt.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= tgt.eff_end_tmstp
    AND SRC.eff_end_tmstp > tgt.eff_end_tmstp ;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE  src.epm_sku_num = tgt.epm_sku_num and 
LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND src.eff_end_tmstp >= tgt.eff_begin_tmstp
    AND src.eff_begin_tmstp < tgt.eff_begin_tmstp
    AND src.eff_end_tmstp < tgt.eff_end_tmstp;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
WITH tbl AS 
(SELECT tgt.*, 
src.eff_begin_tmstp AS src_eff_begin_tmstp, 
src.eff_end_tmstp AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt ON src.epm_sku_num = tgt.epm_sku_num 
AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), tgt_eff_begin_tmstp, src_eff_begin_tmstp FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), src_eff_end_tmstp, tgt_eff_end_tmstp FROM tbl;

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
WHERE EXISTS (SELECT 1 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
WHERE src.epm_sku_num = tgt.epm_sku_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);


-- select count(*),epm_sku_num,channel_country,eff_end_tmstp from {{params.dbenv}}_nap_stg.product_sku_dim_vtw group by epm_sku_num,channel_country,eff_end_tmstp having count(*) > 1


    --SEQUENCED VALIDTIME
DELETE FROM  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
     WHERE epm_sku_num <> tgt.epm_sku_num 
    AND LOWER(channel_country) = LOWER(tgt.channel_country) 
    AND LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(tgt.sku_type_code) = LOWER('P')
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);

UPDATE   `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp,
TGT.eff_end_tmstp_tz = SRC.eff_begin_tmstp_tz
FROM  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
  WHERE src.epm_sku_num <> tgt.epm_sku_num 
    AND LOWER(src.channel_country) = LOWER(tgt.channel_country) 
    AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(tgt.sku_type_code) = LOWER('P')
    AND src.eff_begin_tmstp > tgt.eff_begin_tmstp
    AND src.eff_begin_tmstp <= tgt.eff_end_tmstp
    AND src.eff_end_tmstp > tgt.eff_end_tmstp ;


UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp,
TGT.eff_begin_tmstp_tz = SRC.eff_end_tmstp_tz
FROM  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num <> tgt.epm_sku_num 
    AND LOWER(src.channel_country) = LOWER(tgt.channel_country) 
    AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(tgt.sku_type_code) = LOWER('P')
    AND src.eff_end_tmstp >= tgt.eff_begin_tmstp
    AND src.eff_begin_tmstp < tgt.eff_begin_tmstp
    AND src.eff_end_tmstp < tgt.eff_end_tmstp;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
WITH tbl AS 
(SELECT tgt.*, 
src.eff_begin_tmstp AS src_eff_begin_tmstp, 
src.eff_end_tmstp AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt ON src.epm_sku_num = tgt.epm_sku_num 
AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), tgt_eff_begin_tmstp, src_eff_begin_tmstp FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), src_eff_end_tmstp, tgt_eff_end_tmstp FROM tbl;

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
WHERE EXISTS (SELECT 1 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
WHERE src.epm_sku_num = tgt.epm_sku_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim (rms_sku_num, 
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
 dw_batch_id, 
 dw_batch_date, 
 dw_sys_load_tmstp)
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
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
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





UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt SET
    eff_end_tmstp = SRC.new_end_tmstp ,
    eff_end_tmstp_tz = `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(new_end_tmstp AS STRING))
    FROM (SELECT rms_sku_num,
     channel_country,
      epm_sku_num,
      sku_desc, 
      sku_type_code,
       eff_begin_tmstp, 
       eff_end_tmstp AS old_eff_tmstp,
     MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
        WHERE LOWER(rms_sku_num) <> LOWER('')
        QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country) 
AND SRC.epm_sku_num = tgt.epm_sku_num 
AND SRC.eff_begin_tmstp = tgt.eff_begin_tmstp 
AND SRC.new_end_tmstp > tgt.eff_begin_tmstp;





INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_overlap_stg (new_epm_sku_num, 
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
 cmpy_num, 
 cmpy_desc, 
 color_num, 
 color_desc, 
 nord_display_color, 
 nrf_size_code, 
 size_1_num
 , 
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
 dw_batch_id, 
 dw_batch_date,
 dw_sys_load_tmstp,
 process_flag, 
 new_epm_eff_begin, 
 new_epm_eff_end, 
 old_epm_eff_begin, 
 old_epm_eff_end,
 new_epm_eff_begin_tz,
new_epm_eff_end_tz,
old_epm_eff_begin_tz,
old_epm_eff_end_tz )
(
   SELECT new_epm_sku_num, 
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
 cmpy_num, 
 cmpy_desc, 
 color_num, 
 color_desc, 
 nord_display_color, 
 nrf_size_code, 
 size_1_num
 , 
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
 eff_begin_tmstp_utc as eff_begin_tmstp,
 eff_begin_tmstp_tz, 
 eff_end_tmstp_utc as eff_end_tmstp,
 eff_end_tmstp_tz, 
 dw_batch_id, 
 dw_batch_date,
 dw_sys_load_tmstp,
 process_flag, 
 new_epm_eff_begin, 
 new_epm_eff_end, 
 old_epm_eff_begin, 
 old_epm_eff_end,
`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(new_epm_eff_begin as string)) AS new_epm_eff_begin_tz,
`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(new_epm_eff_end as string)) AS new_epm_eff_end_tz,
`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(old_epm_eff_begin as string)) AS old_epm_eff_begin_tz,
`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(old_epm_eff_end as string)) AS old_epm_eff_end_tz
   FROM(
   SELECT DISTINCT b.epm_sku_num AS new_epm_sku_num,
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
  c.eff_begin_tmstp_utc,
  c.eff_begin_tmstp_tz,
  c.eff_end_tmstp_utc,
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
 WHERE RANGE_CONTAINS(RANGE(B.EFF_BEGIN_TMSTP,B.EFF_END_TMSTP) , RANGE(C.EFF_BEGIN_TMSTP,C.EFF_END_TMSTP)) 
	  AND  
 (a.rms_sku_num, a.channel_country) IN (SELECT (rms_sku_num, channel_country)
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
    GROUP BY rms_sku_num,
     channel_country
    HAVING COUNT(DISTINCT epm_sku_num) > 1)
     AND RANGE_CONTAINS(RANGE(A.EFF_BEGIN_TMSTP_utc,A.EFF_END_TMSTP_utc) ,CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP)))
  WHERE eff_begin_tmstp_UTC >= new_epm_eff_begin);





DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS target
WHERE EXISTS (SELECT *
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg AS source
    WHERE LOWER(target.rms_sku_num) = LOWER(rms_sku_num) AND LOWER(target.channel_country) = LOWER(channel_country) AND target.epm_sku_num = old_epm_sku_num AND target.eff_begin_tmstp >= new_epm_eff_begin_utc AND LOWER(process_flag) = LOWER('N'));








UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS target SET
    eff_begin_tmstp = SOURCE.eff_end_tmstp,
    eff_begin_tmstp_tz = SOURCE.eff_end_tmstp_tz,
    dw_batch_id = (SELECT batch_id
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) FROM (SELECT rms_sku_num, epm_sku_num, channel_country, eff_begin_tmstp_utc as eff_begin_tmstp,eff_end_tmstp_utc as eff_end_tmstp, MIN(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS eff_end_tmstp_prev,eff_end_tmstp_tz
        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
        WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
                    FROM (SELECT DISTINCT rms_sku_num, channel_country
                            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg
                            WHERE LOWER(process_flag) = LOWER('N') AND product_sku_dim_hist.dw_batch_id = (SELECT batch_id
                                        FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
                                        WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))) AS t4)
        QUALIFY eff_begin_tmstp <> (MIN(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))) AS SOURCE
WHERE LOWER(SOURCE.rms_sku_num) = LOWER(target.rms_sku_num) 
AND LOWER(SOURCE.channel_country) = LOWER(target.channel_country) 
AND SOURCE.eff_begin_tmstp = target.eff_begin_tmstp 
AND SOURCE.eff_end_tmstp = target.eff_end_tmstp 
AND CAST(SOURCE.eff_end_tmstp_prev AS TIMESTAMP) <> target.eff_begin_tmstp
;





UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_overlap_stg 
SET
    process_flag = 'Y'
WHERE LOWER(process_flag) = LOWER('N') 
AND dw_batch_id = (SELECT batch_id
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'));



COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;