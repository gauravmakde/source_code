
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=PRODUCT_PARTNER_RELATIONSHIP_DIM;' UPDATE FOR SESSION;*/

BEGIN TRANSACTION;

--NONSEQUENCED VALIDTIME
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_dim_vtw;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_relationship_dim AS target
SET eff_end_tmstp = CAST(SOURCE.effective_end_date_time AS TIMESTAMP),
	eff_end_tmstp_tz = SOURCE.effective_end_date_time_tz
FROM (SELECT dim.partner_relationship_num, dim.eff_begin_tmstp, LDG.effective_end_date_time, LDG.effective_end_date_time_tz
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_relationship_dim AS dim
            INNER JOIN (SELECT DISTINCT partnerrelationshipid AS partner_relationship_num, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectivestartdatetime) AS effective_start_date_time, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveenddatetime) AS effective_end_date_time, `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveenddatetime)) AS effective_end_date_time_tz, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS last_updated_time, recsequencenum AS rec_sequence_num
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_ldg
                QUALIFY (ROW_NUMBER() OVER (PARTITION BY partner_relationship_num ORDER BY rec_sequence_num DESC)) = 1) AS LDG 
				ON LOWER(dim.partner_relationship_num) = LOWER(LDG.partner_relationship_num) AND CAST(dim.eff_end_tmstp AS DATETIME) = DATETIME('9999-12-31 23:59:59.999999')
        WHERE CAST(LDG.effective_end_date_time AS TIMESTAMP) >= dim.eff_begin_tmstp) AS SOURCE
WHERE LOWER(SOURCE.partner_relationship_num) = LOWER(target.partner_relationship_num) 
AND SOURCE.eff_begin_tmstp = target.eff_begin_tmstp;




IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 4') AS `A12180`;
END IF;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_dim_vtw (partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz)

WITH SRC1 AS (
    SELECT 
     partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,eff_begin_tmstp,eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,MIN(eff_begin_tmstp) AS eff_begin_tmstp,MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM        
(	 SELECT DISTINCT
                  src2.partner_relationship_num,
                  src2.partner_relationship_type_code,
                  src2.vendor_num,
                  src2.external_vendor_identifier_source_code,
                  src2.external_vendor_identifier_num,
                  src2.external_vendor_identifier_name,
                  src2.seller_of_record_sold_by_display_name,
                  src2.seller_of_record_fulfilled_by_display_name,
                  src2.return_postal_address_line1_desc,
                  src2.return_postal_address_line2_desc,
                  src2.return_postal_address_line3_desc,
                  src2.return_postal_address_city_code,
                  src2.return_postal_address_state_code,
                  src2.return_postal_address_postal_code,
                  src2.return_postal_address_country_code,
                  src2.customer_care_contact_postal_address_line1_desc,
                  src2.customer_care_contact_postal_address_line2_desc,
                  src2.customer_care_contact_postal_address_line3_desc,
                  src2.customer_care_contact_postal_address_city_code,
                  src2.customer_care_contact_postal_address_state_code,
                  src2.customer_care_contact_postal_address_postal_code,
                  src2.customer_care_contact_postal_address_country_code,
                  src2.customer_care_contact_email_address_desc,
                  src2.customer_care_contact_telephone_num,
                  src2.ship_from_details_postal_address_line1_desc,
                  src2.ship_from_details_postal_address_line2_desc,
                  src2.ship_from_details_postal_address_line3_desc,
                  src2.ship_from_details_postal_address_city_code,
                  src2.ship_from_details_postal_address_state_code,
                  src2.ship_from_details_postal_address_postal_code,
                  src2.ship_from_details_postal_address_country_code,
                  src2.ship_from_details_email_address_desc,
                  src2.ship_from_details_telephone_num,
                  RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period,
                  eff_begin_tmstp,
                  eff_end_tmstp
                FROM
                  (
                    SELECT
                        src1.partner_relationship_num,
                        src1.partner_relationship_type_code,
                        src1.vendor_num,
                        src1.external_vendor_identifier_source_code,
                        src1.external_vendor_identifier_num,
                        src1.external_vendor_identifier_name,
                        src1.seller_of_record_sold_by_display_name,
                        src1.seller_of_record_fulfilled_by_display_name,
                        src1.return_postal_address_line1_desc,
                        src1.return_postal_address_line2_desc,
                        src1.return_postal_address_line3_desc,
                        src1.return_postal_address_city_code,
                        src1.return_postal_address_state_code,
                        src1.return_postal_address_postal_code,
                        src1.return_postal_address_country_code,
                        src1.customer_care_contact_postal_address_line1_desc,
                        src1.customer_care_contact_postal_address_line2_desc,
                        src1.customer_care_contact_postal_address_line3_desc,
                        src1.customer_care_contact_postal_address_city_code,
                        src1.customer_care_contact_postal_address_state_code,
                        src1.customer_care_contact_postal_address_postal_code,
                        src1.customer_care_contact_postal_address_country_code,
                        src1.customer_care_contact_email_address_desc,
                        src1.customer_care_contact_telephone_num,
                        src1.ship_from_details_postal_address_line1_desc,
                        src1.ship_from_details_postal_address_line2_desc,
                        src1.ship_from_details_postal_address_line3_desc,
                        src1.ship_from_details_postal_address_city_code,
                        src1.ship_from_details_postal_address_state_code,
                        src1.ship_from_details_postal_address_postal_code,
                        src1.ship_from_details_postal_address_country_code,
                        src1.ship_from_details_email_address_desc,
                        src1.ship_from_details_telephone_num,
                        CAST(src1.eff_begin_tmstp AS TIMESTAMP) AS eff_begin_tmstp,
                        max(src1.eff_begin_tmstp) OVER (PARTITION BY src1.partner_relationship_num, src1.vendor_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS max_rec_eff_end_tmstp,
                        CASE
                          WHEN src1.effective_end_date_time IS NOT NULL
                           AND max(src1.eff_begin_tmstp) OVER (PARTITION BY src1.partner_relationship_num, src1.vendor_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) IS NULL THEN src1.effective_end_date_time
                          ELSE max(src1.eff_begin_tmstp) OVER (PARTITION BY src1.partner_relationship_num, src1.vendor_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
                        END AS max_eff_end_tmstp,
                        COALESCE(CAST(CASE
                          WHEN src1.effective_end_date_time IS NOT NULL
                           AND max(src1.eff_begin_tmstp) OVER (PARTITION BY src1.partner_relationship_num, src1.vendor_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) IS NULL THEN src1.effective_end_date_time
                          ELSE max(src1.eff_begin_tmstp) OVER (PARTITION BY src1.partner_relationship_num, src1.vendor_num ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
                        END AS TIMESTAMP), TIMESTAMP '9999-12-31 23:59:59.999') AS eff_end_tmstp,
                        src1.last_updated_time
                      FROM
                        (
                          SELECT
                  src0.partner_relationship_num,
                  src0.partner_relationship_type_code,
                  src0.vendor_num,
                  src0.external_vendor_identifier_source_code,
                  src0.external_vendor_identifier_num,
                  src0.external_vendor_identifier_name,
                  src0.seller_of_record_sold_by_display_name,
                  src0.seller_of_record_fulfilled_by_display_name,
                  src0.return_postal_address_line1_desc,
                  src0.return_postal_address_line2_desc,
                  src0.return_postal_address_line3_desc,
                  src0.return_postal_address_city_code,
                  src0.return_postal_address_state_code,
                  src0.return_postal_address_postal_code,
                  src0.return_postal_address_country_code,
                  src0.customer_care_contact_postal_address_line1_desc,
                  src0.customer_care_contact_postal_address_line2_desc,
                  src0.customer_care_contact_postal_address_line3_desc,
                  src0.customer_care_contact_postal_address_city_code,
                  src0.customer_care_contact_postal_address_state_code,
                  src0.customer_care_contact_postal_address_postal_code,
                  src0.customer_care_contact_postal_address_country_code,
                  src0.customer_care_contact_email_address_desc,
                  src0.customer_care_contact_telephone_num,
                  src0.ship_from_details_postal_address_line1_desc,
                  src0.ship_from_details_postal_address_line2_desc,
                  src0.ship_from_details_postal_address_line3_desc,
                  src0.ship_from_details_postal_address_city_code,
                  src0.ship_from_details_postal_address_state_code,
                  src0.ship_from_details_postal_address_postal_code,
                  src0.ship_from_details_postal_address_country_code,
                  src0.ship_from_details_email_address_desc,
                  src0.ship_from_details_telephone_num,
                  CASE
                    WHEN src0.last_updated_time > src0.effective_start_date_time THEN src0.last_updated_time
                    ELSE src0.effective_start_date_time
                  END AS eff_begin_tmstp,
                  src0.effective_end_date_time,
                  src0.last_updated_time,
                  ROW_NUMBER() OVER (PARTITION BY src0.partner_relationship_num, src0.vendor_num, src0.last_updated_time ORDER BY src0.rec_sequence_num DESC) AS row_num
              FROM
                (
                  SELECT DISTINCT
                      product_partner_relationship_ldg.partnerrelationshipid AS partner_relationship_num,
                      product_partner_relationship_ldg.partnerrelationshiptype AS partner_relationship_type_code,
                      product_partner_relationship_ldg.vendornumber AS vendor_num,
                      product_partner_relationship_ldg.externalvendoridentifiersource AS external_vendor_identifier_source_code,
                      product_partner_relationship_ldg.externalvendoridentifierid AS external_vendor_identifier_num,
                      product_partner_relationship_ldg.externalvendoridentifiername AS external_vendor_identifier_name,
                      product_partner_relationship_ldg.sellerofrecordsoldbydisplayname AS seller_of_record_sold_by_display_name,
                      product_partner_relationship_ldg.sellerofrecordfulfilledbydisplayname AS seller_of_record_fulfilled_by_display_name,
                      product_partner_relationship_ldg.returnpostaladdressline1 AS return_postal_address_line1_desc,
                      product_partner_relationship_ldg.returnpostaladdressline2 AS return_postal_address_line2_desc,
                      product_partner_relationship_ldg.returnpostaladdressline3 AS return_postal_address_line3_desc,
                      product_partner_relationship_ldg.returnpostaladdresscity AS return_postal_address_city_code,
                      product_partner_relationship_ldg.returnpostaladdressstate AS return_postal_address_state_code,
                      product_partner_relationship_ldg.returnpostaladdresspostalcode AS return_postal_address_postal_code,
                      product_partner_relationship_ldg.returnpostaladdresscountrycode AS return_postal_address_country_code,
                      product_partner_relationship_ldg.customercarecontactpostaladdressline1 AS customer_care_contact_postal_address_line1_desc,
                      product_partner_relationship_ldg.customercarecontactpostaladdressline2 AS customer_care_contact_postal_address_line2_desc,
                      product_partner_relationship_ldg.customercarecontactpostaladdressline3 AS customer_care_contact_postal_address_line3_desc,
                      product_partner_relationship_ldg.customercarecontactpostaladdresscity AS customer_care_contact_postal_address_city_code,
                      product_partner_relationship_ldg.customercarecontactpostaladdressstate AS customer_care_contact_postal_address_state_code,
                      product_partner_relationship_ldg.customercarecontactpostaladdresspostalcode AS customer_care_contact_postal_address_postal_code,
                      product_partner_relationship_ldg.customercarecontactpostaladdresscountrycode AS customer_care_contact_postal_address_country_code,
                      product_partner_relationship_ldg.customercarecontactemailaddress AS customer_care_contact_email_address_desc,
                      product_partner_relationship_ldg.customercarecontacttelephonenumber AS customer_care_contact_telephone_num,
                      product_partner_relationship_ldg.shipfromdetailspostaladdressline1 AS ship_from_details_postal_address_line1_desc,
                      product_partner_relationship_ldg.shipfromdetailspostaladdressline2 AS ship_from_details_postal_address_line2_desc,
                      product_partner_relationship_ldg.shipfromdetailspostaladdressline3 AS ship_from_details_postal_address_line3_desc,
                      product_partner_relationship_ldg.shipfromdetailspostaladdresscity AS ship_from_details_postal_address_city_code,
                      product_partner_relationship_ldg.shipfromdetailspostaladdressstate AS ship_from_details_postal_address_state_code,
                      product_partner_relationship_ldg.shipfromdetailspostaladdresspostalcode AS ship_from_details_postal_address_postal_code,
                      product_partner_relationship_ldg.shipfromdetailspostaladdresscountrycode AS ship_from_details_postal_address_country_code,
                      product_partner_relationship_ldg.shipfromdetailsemailaddress AS ship_from_details_email_address_desc,
                      product_partner_relationship_ldg.shipfromdetailstelephonenumber AS ship_from_details_telephone_num,
                       `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(product_partner_relationship_ldg.effectiveStartDateTime) AS effective_start_date_time
                        , `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(product_partner_relationship_ldg.effectiveEndDateTime) AS effective_end_date_time
                        , `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(product_partner_relationship_ldg.lastUpdatedTime) AS last_updated_time
                        , product_partner_relationship_ldg.recSequenceNum AS rec_sequence_num
                    FROM
                      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_ldg
                ) AS src0
                            QUALIFY row_num = 1
                        ) AS src1
                  ) AS src2
                WHERE CAST(src2.eff_begin_tmstp AS TIMESTAMP) < src2.eff_end_tmstp
             
			 
 )) AS ordered_data
            ) AS grouped_data
            GROUP BY 
                partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,range_group
            ORDER BY  
                partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,eff_begin_tmstp
        )
    )
	
	
	
  SELECT
      nrml.partner_relationship_num,      
	  nrml.partner_relationship_type_code,
      nrml.vendor_num,
      nrml.external_vendor_identifier_source_code,
      nrml.external_vendor_identifier_num,
      nrml.external_vendor_identifier_name,
      nrml.seller_of_record_sold_by_display_name,
      nrml.seller_of_record_fulfilled_by_display_name,
      nrml.return_postal_address_line1_desc,
      nrml.return_postal_address_line2_desc,
      nrml.return_postal_address_line3_desc,
      nrml.return_postal_address_city_code,
      nrml.return_postal_address_state_code,
      nrml.return_postal_address_postal_code,
      nrml.return_postal_address_country_code,
      nrml.customer_care_contact_postal_address_line1_desc,
      nrml.customer_care_contact_postal_address_line2_desc,
      nrml.customer_care_contact_postal_address_line3_desc,
      nrml.customer_care_contact_postal_address_city_code,
      nrml.customer_care_contact_postal_address_state_code,
      nrml.customer_care_contact_postal_address_postal_code,
      nrml.customer_care_contact_postal_address_country_code,
      nrml.customer_care_contact_email_address_desc,
      nrml.customer_care_contact_telephone_num,
      nrml.ship_from_details_postal_address_line1_desc,
      nrml.ship_from_details_postal_address_line2_desc,
      nrml.ship_from_details_postal_address_line3_desc,
      nrml.ship_from_details_postal_address_city_code,
      nrml.ship_from_details_postal_address_state_code,
      nrml.ship_from_details_postal_address_postal_code,
      nrml.ship_from_details_postal_address_country_code,
      nrml.ship_from_details_email_address_desc,
      nrml.ship_from_details_telephone_num,
	  eff_begin_tmstp,
	  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(eff_begin_tmstp as string)) as eff_begin_tmstp_tz,
	  eff_end_tmstp,
	  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(eff_end_tmstp as string)) as eff_begin_tmstp_tz
    FROM
      (
        --NONSEQUENCED VALIDTIME
		---NORMALIZE
    SELECT 
     partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,eff_begin_tmstp,eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num,
			eff_period,
			MIN(RANGE_START(eff_period)) AS eff_begin_tmstp,
			MAX(RANGE_END(eff_period)) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num,
					eff_period
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num,
							eff_period
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
SELECT DISTINCT
            src.partner_relationship_num,
            src.partner_relationship_type_code,
            src.vendor_num,
            src.external_vendor_identifier_source_code,
            src.external_vendor_identifier_num,
            src.external_vendor_identifier_name,
            src.seller_of_record_sold_by_display_name,
            src.seller_of_record_fulfilled_by_display_name,
            src.return_postal_address_line1_desc,
            src.return_postal_address_line2_desc,
            src.return_postal_address_line3_desc,
            src.return_postal_address_city_code,
            src.return_postal_address_state_code,
            src.return_postal_address_postal_code,
            src.return_postal_address_country_code,
            src.customer_care_contact_postal_address_line1_desc,
            src.customer_care_contact_postal_address_line2_desc,
            src.customer_care_contact_postal_address_line3_desc,
            src.customer_care_contact_postal_address_city_code,
            src.customer_care_contact_postal_address_state_code,
            src.customer_care_contact_postal_address_postal_code,
            src.customer_care_contact_postal_address_country_code,
            src.customer_care_contact_email_address_desc,
            src.customer_care_contact_telephone_num,
            src.ship_from_details_postal_address_line1_desc,
            src.ship_from_details_postal_address_line2_desc,
            src.ship_from_details_postal_address_line3_desc,
            src.ship_from_details_postal_address_city_code,
            src.ship_from_details_postal_address_state_code,
            src.ship_from_details_postal_address_postal_code,
            src.ship_from_details_postal_address_country_code,
            src.ship_from_details_email_address_desc,
            src.ship_from_details_telephone_num,
			src.eff_end_tmstp,
			src.eff_begin_tmstp,
            COALESCE( RANGE_INTERSECT(src.eff_period, RANGE(CAST(tgt.eff_begin_tmstp AS TIMESTAMP),CAST(tgt.eff_end_tmstp AS TIMESTAMP)) )) AS eff_period
          FROM
            (
				select * ---,RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
     from SRC1
			) AS src
            LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_partner_relationship_dim AS tgt ON src.partner_relationship_num = tgt.partner_relationship_num
             AND RANGE_OVERLAPS(src.eff_period, RANGE(CAST(tgt.eff_begin_tmstp AS TIMESTAMP), CAST(tgt.eff_end_tmstp AS TIMESTAMP)))
          WHERE ( tgt.partner_relationship_num IS NULL
           OR ((src.partner_relationship_type_code <> tgt.partner_relationship_type_code
           OR (src.partner_relationship_type_code IS NOT NULL
           AND tgt.partner_relationship_type_code IS NULL)
           OR (src.partner_relationship_type_code IS NULL
           AND tgt.partner_relationship_type_code IS NOT NULL))
           OR (src.vendor_num <> tgt.vendor_num
           OR (src.vendor_num IS NOT NULL
           AND tgt.vendor_num IS NULL)
           OR (src.vendor_num IS NULL
           AND tgt.vendor_num IS NOT NULL))
           OR (src.external_vendor_identifier_source_code <> tgt.external_vendor_identifier_source_code
           OR (src.external_vendor_identifier_source_code IS NOT NULL
           AND tgt.external_vendor_identifier_source_code IS NULL)
           OR (src.external_vendor_identifier_source_code IS NULL
           AND tgt.external_vendor_identifier_source_code IS NOT NULL))
           OR (src.external_vendor_identifier_num <> tgt.external_vendor_identifier_num
           OR (src.external_vendor_identifier_num IS NOT NULL
           AND tgt.external_vendor_identifier_num IS NULL)
           OR (src.external_vendor_identifier_num IS NULL
           AND tgt.external_vendor_identifier_num IS NOT NULL))
           OR (src.external_vendor_identifier_name <> tgt.external_vendor_identifier_name
           OR (src.external_vendor_identifier_name IS NOT NULL
           AND tgt.external_vendor_identifier_name IS NULL)
           OR (src.external_vendor_identifier_name IS NULL
           AND tgt.external_vendor_identifier_name IS NOT NULL))
           OR (src.seller_of_record_sold_by_display_name <> tgt.seller_of_record_sold_by_display_name
           OR (src.seller_of_record_sold_by_display_name IS NOT NULL
           AND tgt.seller_of_record_sold_by_display_name IS NULL)
           OR (src.seller_of_record_sold_by_display_name IS NULL
           AND tgt.seller_of_record_sold_by_display_name IS NOT NULL))
           OR (src.seller_of_record_fulfilled_by_display_name <> tgt.seller_of_record_fulfilled_by_display_name
           OR (src.seller_of_record_fulfilled_by_display_name IS NOT NULL
           AND tgt.seller_of_record_fulfilled_by_display_name IS NULL)
           OR (src.seller_of_record_fulfilled_by_display_name IS NULL
           AND tgt.seller_of_record_fulfilled_by_display_name IS NOT NULL))
           OR (src.return_postal_address_line1_desc <> tgt.return_postal_address_line1_desc
           OR (src.return_postal_address_line1_desc IS NOT NULL
           AND tgt.return_postal_address_line1_desc IS NULL)
           OR (src.return_postal_address_line1_desc IS NULL
           AND tgt.return_postal_address_line1_desc IS NOT NULL))
           OR (src.return_postal_address_line2_desc <> tgt.return_postal_address_line2_desc
           OR (src.return_postal_address_line2_desc IS NOT NULL
           AND tgt.return_postal_address_line2_desc IS NULL)
           OR (src.return_postal_address_line2_desc IS NULL
           AND tgt.return_postal_address_line2_desc IS NOT NULL))
           OR (src.return_postal_address_line3_desc <> tgt.return_postal_address_line3_desc
           OR (src.return_postal_address_line3_desc IS NOT NULL
           AND tgt.return_postal_address_line3_desc IS NULL)
           OR (src.return_postal_address_line3_desc IS NULL
           AND tgt.return_postal_address_line3_desc IS NOT NULL))
           OR (src.return_postal_address_city_code <> tgt.return_postal_address_city_code
           OR (src.return_postal_address_city_code IS NOT NULL
           AND tgt.return_postal_address_city_code IS NULL)
           OR (src.return_postal_address_city_code IS NULL
           AND tgt.return_postal_address_city_code IS NOT NULL))
           OR (src.return_postal_address_state_code <> tgt.return_postal_address_state_code
           OR (src.return_postal_address_state_code IS NOT NULL
           AND tgt.return_postal_address_state_code IS NULL)
           OR (src.return_postal_address_state_code IS NULL
           AND tgt.return_postal_address_state_code IS NOT NULL))
           OR (src.return_postal_address_postal_code <> tgt.return_postal_address_postal_code
           OR (src.return_postal_address_postal_code IS NOT NULL
           AND tgt.return_postal_address_postal_code IS NULL)
           OR (src.return_postal_address_postal_code IS NULL
           AND tgt.return_postal_address_postal_code IS NOT NULL))
           OR (src.return_postal_address_country_code <> tgt.return_postal_address_country_code
           OR (src.return_postal_address_country_code IS NOT NULL
           AND tgt.return_postal_address_country_code IS NULL)
           OR (src.return_postal_address_country_code IS NULL
           AND tgt.return_postal_address_country_code IS NOT NULL))
           OR (src.customer_care_contact_postal_address_line1_desc <> tgt.customer_care_contact_postal_address_line1_desc
           OR (src.customer_care_contact_postal_address_line1_desc IS NOT NULL
           AND tgt.customer_care_contact_postal_address_line1_desc IS NULL)
           OR (src.customer_care_contact_postal_address_line1_desc IS NULL
           AND tgt.customer_care_contact_postal_address_line1_desc IS NOT NULL))
           OR (src.customer_care_contact_postal_address_line2_desc <> tgt.customer_care_contact_postal_address_line2_desc
           OR (src.customer_care_contact_postal_address_line2_desc IS NOT NULL
           AND tgt.customer_care_contact_postal_address_line2_desc IS NULL)
           OR (src.customer_care_contact_postal_address_line2_desc IS NULL
           AND tgt.customer_care_contact_postal_address_line2_desc IS NOT NULL))
           OR (src.customer_care_contact_postal_address_line3_desc <> tgt.customer_care_contact_postal_address_line3_desc
           OR (src.customer_care_contact_postal_address_line3_desc IS NOT NULL
           AND tgt.customer_care_contact_postal_address_line3_desc IS NULL)
           OR (src.customer_care_contact_postal_address_line3_desc IS NULL
           AND tgt.customer_care_contact_postal_address_line3_desc IS NOT NULL))
           OR (src.customer_care_contact_postal_address_city_code <> tgt.customer_care_contact_postal_address_city_code
           OR (src.customer_care_contact_postal_address_city_code IS NOT NULL
           AND tgt.customer_care_contact_postal_address_city_code IS NULL)
           OR (src.customer_care_contact_postal_address_city_code IS NULL
           AND tgt.customer_care_contact_postal_address_city_code IS NOT NULL))
           OR (src.customer_care_contact_postal_address_state_code <> tgt.customer_care_contact_postal_address_state_code
           OR (src.customer_care_contact_postal_address_state_code IS NOT NULL
           AND tgt.customer_care_contact_postal_address_state_code IS NULL)
           OR (src.customer_care_contact_postal_address_state_code IS NULL
           AND tgt.customer_care_contact_postal_address_state_code IS NOT NULL))
           OR (src.customer_care_contact_postal_address_postal_code <> tgt.customer_care_contact_postal_address_postal_code
           OR (src.customer_care_contact_postal_address_postal_code IS NOT NULL
           AND tgt.customer_care_contact_postal_address_postal_code IS NULL)
           OR (src.customer_care_contact_postal_address_postal_code IS NULL
           AND tgt.customer_care_contact_postal_address_postal_code IS NOT NULL))
           OR (src.customer_care_contact_postal_address_country_code <> tgt.customer_care_contact_postal_address_country_code
           OR (src.customer_care_contact_postal_address_country_code IS NOT NULL
           AND tgt.customer_care_contact_postal_address_country_code IS NULL)
           OR (src.customer_care_contact_postal_address_country_code IS NULL
           AND tgt.customer_care_contact_postal_address_country_code IS NOT NULL))
           OR (src.customer_care_contact_email_address_desc <> tgt.customer_care_contact_email_address_desc
           OR (src.customer_care_contact_email_address_desc IS NOT NULL
           AND tgt.customer_care_contact_email_address_desc IS NULL)
           OR (src.customer_care_contact_email_address_desc IS NULL
           AND tgt.customer_care_contact_email_address_desc IS NOT NULL))
           OR (src.customer_care_contact_telephone_num <> tgt.customer_care_contact_telephone_num
           OR (src.customer_care_contact_telephone_num IS NOT NULL
           AND tgt.customer_care_contact_telephone_num IS NULL)
           OR (src.customer_care_contact_telephone_num IS NULL
           AND tgt.customer_care_contact_telephone_num IS NOT NULL))
           OR (src.ship_from_details_postal_address_line1_desc <> tgt.ship_from_details_postal_address_line1_desc
           OR (src.ship_from_details_postal_address_line1_desc IS NOT NULL
           AND tgt.ship_from_details_postal_address_line1_desc IS NULL)
           OR (src.ship_from_details_postal_address_line1_desc IS NULL
           AND tgt.ship_from_details_postal_address_line1_desc IS NOT NULL))
           OR (src.ship_from_details_postal_address_line2_desc <> tgt.ship_from_details_postal_address_line2_desc
           OR (src.ship_from_details_postal_address_line2_desc IS NOT NULL
           AND tgt.ship_from_details_postal_address_line2_desc IS NULL)
           OR (src.ship_from_details_postal_address_line2_desc IS NULL
           AND tgt.ship_from_details_postal_address_line2_desc IS NOT NULL))
           OR (src.ship_from_details_postal_address_line3_desc <> tgt.ship_from_details_postal_address_line3_desc
           OR (src.ship_from_details_postal_address_line3_desc IS NOT NULL
           AND tgt.ship_from_details_postal_address_line3_desc IS NULL)
           OR (src.ship_from_details_postal_address_line3_desc IS NULL
           AND tgt.ship_from_details_postal_address_line3_desc IS NOT NULL))
           OR (src.ship_from_details_postal_address_city_code <> tgt.ship_from_details_postal_address_city_code
           OR (src.ship_from_details_postal_address_city_code IS NOT NULL
           AND tgt.ship_from_details_postal_address_city_code IS NULL)
           OR (src.ship_from_details_postal_address_city_code IS NULL
           AND tgt.ship_from_details_postal_address_city_code IS NOT NULL))
           OR (src.ship_from_details_postal_address_state_code <> tgt.ship_from_details_postal_address_state_code
           OR (src.ship_from_details_postal_address_state_code IS NOT NULL
           AND tgt.ship_from_details_postal_address_state_code IS NULL)
           OR (src.ship_from_details_postal_address_state_code IS NULL
           AND tgt.ship_from_details_postal_address_state_code IS NOT NULL))
           OR (src.ship_from_details_postal_address_postal_code <> tgt.ship_from_details_postal_address_postal_code
           OR (src.ship_from_details_postal_address_postal_code IS NOT NULL
           AND tgt.ship_from_details_postal_address_postal_code IS NULL)
           OR (src.ship_from_details_postal_address_postal_code IS NULL
           AND tgt.ship_from_details_postal_address_postal_code IS NOT NULL))
           OR (src.ship_from_details_postal_address_country_code <> tgt.ship_from_details_postal_address_country_code
           OR (src.ship_from_details_postal_address_country_code IS NOT NULL
           AND tgt.ship_from_details_postal_address_country_code IS NULL)
           OR (src.ship_from_details_postal_address_country_code IS NULL
           AND tgt.ship_from_details_postal_address_country_code IS NOT NULL))
           OR (src.ship_from_details_email_address_desc <> tgt.ship_from_details_email_address_desc
           OR (src.ship_from_details_email_address_desc IS NOT NULL
           AND tgt.ship_from_details_email_address_desc IS NULL)
           OR (src.ship_from_details_email_address_desc IS NULL
           AND tgt.ship_from_details_email_address_desc IS NOT NULL))
           OR (src.ship_from_details_telephone_num <> tgt.ship_from_details_telephone_num
           OR (src.ship_from_details_telephone_num IS NOT NULL
           AND tgt.ship_from_details_telephone_num IS NULL)
           OR (src.ship_from_details_telephone_num IS NULL
           AND tgt.ship_from_details_telephone_num IS NOT NULL))
		))
      
	  
 )) AS ordered_data
            ) AS grouped_data
            GROUP BY 
                partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,range_group
            ORDER BY  
                partner_relationship_num, partner_relationship_type_code, vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name, seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc, return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code, return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code, customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc, customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code, customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code, customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc, customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc, ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc, ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code, ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code, ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_period,eff_begin_tmstp
        )
    )
 AS nrml
	  
	  

;
-- -convert this teradata code into bigquery compatible





---SEQUENCED VALIDTIME

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_relationship_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_dim_vtw AS src
    WHERE LOWER(src.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
	AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_relationship_dim AS tgt
SET tgt.eff_end_tmstp = src.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_dim_vtw AS src
WHERE LOWER(src.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
    AND src.eff_begin_tmstp >= tgt.eff_begin_tmstp
    AND src.eff_begin_tmstp < tgt.eff_end_tmstp 
    AND src.eff_end_tmstp > tgt.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_relationship_dim AS tgt
SET tgt.eff_begin_tmstp = src.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_dim_vtw AS src
WHERE LOWER(src.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
    AND src.eff_end_tmstp > tgt.eff_begin_tmstp
    AND src.eff_end_tmstp <= tgt.eff_end_tmstp;

	
IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 7') AS `A12180`;
END IF;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_relationship_dim (partner_relationship_num, partner_relationship_type_code,
 vendor_num, external_vendor_identifier_source_code, external_vendor_identifier_num, external_vendor_identifier_name,
 seller_of_record_sold_by_display_name, seller_of_record_fulfilled_by_display_name, return_postal_address_line1_desc,
 return_postal_address_line2_desc, return_postal_address_line3_desc, return_postal_address_city_code,
 return_postal_address_state_code, return_postal_address_postal_code, return_postal_address_country_code,
 customer_care_contact_postal_address_line1_desc, customer_care_contact_postal_address_line2_desc,
 customer_care_contact_postal_address_line3_desc, customer_care_contact_postal_address_city_code,
 customer_care_contact_postal_address_state_code, customer_care_contact_postal_address_postal_code,
 customer_care_contact_postal_address_country_code, customer_care_contact_email_address_desc,
 customer_care_contact_telephone_num, ship_from_details_postal_address_line1_desc,
 ship_from_details_postal_address_line2_desc, ship_from_details_postal_address_line3_desc,
 ship_from_details_postal_address_city_code, ship_from_details_postal_address_state_code,
 ship_from_details_postal_address_postal_code, ship_from_details_postal_address_country_code,
 ship_from_details_email_address_desc, ship_from_details_telephone_num, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp, eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_date, dw_sys_load_tmstp)
(SELECT partner_relationship_num,
  partner_relationship_type_code,
  vendor_num,
  external_vendor_identifier_source_code,
  external_vendor_identifier_num,
  external_vendor_identifier_name,
  seller_of_record_sold_by_display_name,
  seller_of_record_fulfilled_by_display_name,
  return_postal_address_line1_desc,
  return_postal_address_line2_desc,
  return_postal_address_line3_desc,
  return_postal_address_city_code,
  return_postal_address_state_code,
  return_postal_address_postal_code,
  return_postal_address_country_code,
  customer_care_contact_postal_address_line1_desc,
  customer_care_contact_postal_address_line2_desc,
  customer_care_contact_postal_address_line3_desc,
  customer_care_contact_postal_address_city_code,
  customer_care_contact_postal_address_state_code,
  customer_care_contact_postal_address_postal_code,
  customer_care_contact_postal_address_country_code,
  customer_care_contact_email_address_desc,
  customer_care_contact_telephone_num,
  ship_from_details_postal_address_line1_desc,
  ship_from_details_postal_address_line2_desc,
  ship_from_details_postal_address_line3_desc,
  ship_from_details_postal_address_city_code,
  ship_from_details_postal_address_state_code,
  ship_from_details_postal_address_postal_code,
  ship_from_details_postal_address_country_code,
  ship_from_details_email_address_desc,
  ship_from_details_telephone_num,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  CAST(CASE
    WHEN RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') = ''
    THEN '0'
    ELSE RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ')
    END AS BIGINT) AS dw_batch_id,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_relationship_dim_vtw);

IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 8') AS `A12180`;
END IF;

COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;

IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 9') AS `A12180`;
END IF;

--COLLECT STATISTICS COLUMN ( partner_relationship_num ) , COLUMN ( external_vendor_identifier_num ) , COLUMN ( vendor_num ) , COLUMN ( eff_begin_tmstp ) , COLUMN ( eff_end_tmstp ) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_DIM.PRODUCT_PARTNER_RELATIONSHIP_DIM;
END;
