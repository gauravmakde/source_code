/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08907;
     DAG_ID=fashion_map_choice_11521_ACE_ENG;
     Task_Name=fashion_map_choice;'
     FOR SESSION VOLATILE; 

/*
T2/Table Name: T2DL_DAS_DIGENG.FASHION_MAP_CHOICE | T2DL_DAS_DIGENG.FASHION_MAP_CHOICE_IMAGES | T2DL_DAS_DIGENG.FASHION_MAP_CHOICE_EMBEDDING_UPDATES | T2DL_DAS_DIGENG.FASHION_MAP_CHOICE_STATUS_UPDATES | T2DL_DAS_DIGENG.FASHION_MAP_CHOICE_NEW
Team/Owner: AI Digital/Soren Stime, Robert Legg
Date Created/Modified: 10/03/2024
 
Note: Choice level data for fashion map users to join embeddings to so they know what product they are looking at. 
This sql runs the code for three tables, FASHION_MAP_CHOICE, FASHION_MAP_CHOICE_UPDATES, and FASHION_MAP_CHOICE_NEW.

*/
-- Get the data to choice level, count skus for later ranking, add a random number if a tie occurs (super rare) 
    
CREATE MULTISET VOLATILE TABLE vt_fashionmap_choice AS (  
SELECT  
	channel_country,  
	channel_brand,  
	selling_channel,  
	trim(web_style_num) as web_style_num,  
	trim(color_num) as color_num,  
	rms_style_num,  
	COUNT(DISTINCT rms_sku_num) AS rms_sku_count,  
	MAX(selling_status_code) AS selling_status_code,  
	MAX(selling_channel_eligibility_code) AS selling_channel_eligibility_code,  
	MAX(
		case 
			when is_online_purchasable is null then 'N'
		else is_online_purchasable
		end
		) AS is_online_purchasable,
	MAX(selling_retail_price_amt) as selling_retail_price_amt,
	MAX(regular_price_amt) as regular_price_amt,  
	MAX(vendor_brand_name) AS vendor_brand_name,  
	MAX(UPPER(trim(type_level_1_desc))) AS type_level_1_desc,  
	MAX(UPPER(trim(type_level_2_desc))) AS type_level_2_desc,  
	MAX(genders_desc) AS genders_desc,  
	MAX(age_groups_desc) AS age_groups_desc,  
	MAX(title_desc) AS title_desc,  
	MAX(features_desc) AS features_desc,  
	MAX(details_and_care_desc) AS details_and_care_desc,  
	MAX(ingredients_desc) AS ingredients_desc,  
	(RANDOM(0, 1000000) / 1000000.00000) AS random_num  
FROM PRD_NAP_USR_VWS.PRODUCT_FASHIONMAP_SKU_DIM  
where channel_brand = 'NORDSTROM'
GROUP BY  
	channel_country,  
	channel_brand,  
	selling_channel,  
	web_style_num,  
	color_num,  
	rms_style_num  
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS
;  
  
-- Dedupe data and if a web_style_num has more than one rms_style_num pick the id with the most skus associated with it
-- now choice should be perfect join, always 1 record per choice
-- THIS IS THE NEWEST VERSION OF CHOICE DATA 
CREATE MULTISET VOLATILE TABLE vt_choice_dedupe AS (  
SELECT  
	trim(fashionmap.web_style_num) as web_style_num,
	fashionmap.rms_style_num, 
	fashionmap.color_num,  
	fashionmap.channel_country,  
	fashionmap.channel_brand,   
	fashionmap.selling_channel,  
	fashionmap.selling_status_code,  
	fashionmap.selling_channel_eligibility_code,  
	fashionmap.is_online_purchasable,
	fashionmap.selling_retail_price_amt,
	fashionmap.regular_price_amt,
	fashionmap.vendor_brand_name,  
	CASE
		WHEN REGEXP_SIMILAR (fashionmap.type_level_1_desc, '.*[0-9].*') = 1 THEN 'UNKNOWN'  
		WHEN fashionmap.type_level_1_desc is null then 'N/A'
		ELSE fashionmap.type_level_1_desc  
	END AS type_level_1_desc,  
	CASE   
		WHEN fashionmap.type_level_2_desc = '2 PIECE SETS' THEN fashionmap.type_level_2_desc
		WHEN fashionmap.type_level_2_desc = '3/4 OR LONG COAT' THEN fashionmap.type_level_2_desc
		WHEN fashionmap.type_level_2_desc = '3 PIECE SETS' THEN fashionmap.type_level_2_desc
		WHEN fashionmap.type_level_2_desc = '4 PIECE SETS' THEN fashionmap.type_level_2_desc
		WHEN REGEXP_SIMILAR (fashionmap.type_level_2_desc, '.*[0-9].*') = 1 THEN 'UNKNOWN'
		WHEN fashionmap.type_level_2_desc is null then 'N/A'  
		ELSE fashionmap.type_level_2_desc  
	END AS type_level_2_desc,
	fashionmap.genders_desc,  
	fashionmap.age_groups_desc,  
	fashionmap.title_desc,  
	fashionmap.features_desc,  
	fashionmap.details_and_care_desc,  
	fashionmap.ingredients_desc
FROM (  
	SELECT
		channel_country,  
		channel_brand,  
		selling_channel,  
		web_style_num,  
		color_num,  
		rms_style_num,  
		rms_sku_count,  
		selling_status_code,  
		selling_channel_eligibility_code,  
		is_online_purchasable,
		selling_retail_price_amt,
		regular_price_amt,
		vendor_brand_name,  
		type_level_1_desc,  
		type_level_2_desc,  
		genders_desc,  
		age_groups_desc,  
		title_desc,  
		features_desc,  
		details_and_care_desc,  
		ingredients_desc,  
		random_num,
		ROW_NUMBER() OVER (PARTITION BY web_style_num, color_num ORDER BY rms_sku_count DESC, random_num) AS rn  
	FROM vt_fashionmap_choice  
) fashionmap  
WHERE rn = 1
QUALIFY ROW_NUMBER() 
	OVER 
	(PARTITION BY fashionmap.channel_country, fashionmap.channel_brand, fashionmap.selling_channel, fashionmap.web_style_num, fashionmap.color_num, fashionmap.rms_style_num 
	ORDER BY fashionmap.channel_country, fashionmap.channel_brand, fashionmap.selling_channel, fashionmap.web_style_num, fashionmap.color_num, fashionmap.rms_style_num
	) = 1  
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS
;  

-- image table prep
---- Reproduct the above choice logic for images, needs to be a perfect join just like above 
---- We are not including rms_style_num in the image table as rms_style_num is typically used in in store only products. So they wouldn't have a image anyway. 
------ Might need to come back to this logic in the future

CREATE MULTISET VOLATILE TABLE vt_image_choice AS (  
select 
	distinct 
	channel_country,  
	channel_brand,  
	web_style_num,  
	color_num, 
	trim(image_index) as image_index,  
	is_hero_image,  
	asset_id,  
	image_url  
from PRD_NAP_USR_VWS.PRODUCT_FASHIONMAP_SKU_IMAGE_ASSET_VW
WHERE channel_country = 'US'  
and channel_brand = 'NORDSTROM'
) WITH DATA 
PRIMARY INDEX (asset_id) 
ON COMMIT PRESERVE ROWS
;  

-- shot_name preparation 
---- 75% of the time an asset_id has only 1 associated shot_name so we need to fix this to be always one or it will duplicate rows
---- Most of the time it is one record with a null shot_name and one with a non-null shot_name
---- Priority is given to the non-null shot_name, if multiple not null then randomly pick one
---- When more than 2 records have the same asset_id, channel_brand, color_num, image_index, and is_hero_image, it is usually bad data so I think random is fine in this case 
CREATE MULTISET VOLATILE TABLE vt_image_prep AS (  
select 
	asset_id,  
	channel_brand,  
	color_num,  
	image_index,  
	is_hero_image,  
	shot_name,
	ROW_NUMBER() OVER (PARTITION BY asset_id, channel_brand, color_num, image_index, is_hero_image ORDER BY priority asc, random_num) AS rn
from (
	SELECT
		asset_id,  
		channel_brand,  
		color_num,  
		image_index,  
		is_hero_image,  
		shot_name,
		CASE   
        	WHEN shot_name IS NOT NULL THEN 1   
            ELSE 2   
        END as priority,
		(RANDOM(0, 1000000) / 1000000.00000) AS random_num 
	FROM PRD_NAP_USR_VWS.PRODUCT_IMAGE_ASSET_DIM  
	WHERE channel_country = 'US'  
	and channel_brand = 'NORDSTROM'
	GROUP BY
		asset_id,  
		channel_brand,  
		color_num,  
		image_index,  
		is_hero_image,  
		shot_name,
		priority
	) a   
) WITH DATA 
PRIMARY INDEX (asset_id) 
ON COMMIT PRESERVE ROWS
;  


-- New image table 
CREATE MULTISET VOLATILE TABLE vt_image_final as (
select 
	distinct
	img_base.channel_country,  
	img_base.channel_brand,  
	trim(img_base.web_style_num) as web_style_num,  
	trim(img_base.color_num) as color_num, 
	img_base.image_index,  
	img_base.is_hero_image,  
	img_base.asset_id,  
	img_base.image_url,
	shot_name.shot_name
from vt_image_choice img_base
left join vt_image_prep shot_name
	ON img_base.asset_id = shot_name.asset_id  
		AND img_base.image_index = shot_name.image_index  
		AND img_base.is_hero_image = shot_name.is_hero_image  
		AND img_base.channel_brand = shot_name.channel_brand
		AND img_base.color_num = shot_name.color_num
where shot_name.rn = 1
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS
;

-- This would be the most up to date version of choice and image joined together, we will compare to the same joined table from yesterday
CREATE MULTISET VOLATILE TABLE vt_new_choice AS (  
SELECT  
	distinct 
	trim(choice.web_style_num) as web_style_num,
	choice.rms_style_num,
	choice.color_num,  
	choice.channel_country,  
	choice.channel_brand,   
	choice.selling_channel,  
	choice.selling_status_code,  
	choice.selling_channel_eligibility_code,  
	choice.is_online_purchasable,
	choice.selling_retail_price_amt,
	choice.regular_price_amt,  
	choice.vendor_brand_name,  
	choice.type_level_1_desc,  
	choice.type_level_2_desc,  
	choice.genders_desc,  
	choice.age_groups_desc,  
	choice.title_desc,  
	choice.features_desc,  
	choice.details_and_care_desc,  
	choice.ingredients_desc,  
	image.image_index,  
	image.shot_name,  
	image.is_hero_image,  
	image.asset_id,  
	image.image_url
FROM vt_choice_dedupe choice  
inner JOIN vt_image_final image  
ON trim(choice.web_style_num) = trim(image.web_style_num)  
AND trim(choice.color_num) = trim(image.color_num)
AND trim(choice.channel_brand) = trim(image.channel_brand)
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS;

-- This would be the most up to date version of choice and image joined together, we will compare to the same joined table from yesterday
CREATE MULTISET VOLATILE TABLE vt_old_choice AS (  
SELECT  
	distinct 
	trim(choice.web_style_num) as web_style_num,
	choice.rms_style_num,
	choice.color_num,  
	choice.channel_country,  
	choice.channel_brand,   
	choice.selling_channel,  
	choice.selling_status_code,  
	choice.selling_channel_eligibility_code,  
	choice.is_online_purchasable,
	choice.selling_retail_price_amt,
	choice.regular_price_amt,  
	choice.vendor_brand_name,  
	choice.type_level_1_desc,  
	choice.type_level_2_desc,  
	choice.genders_desc,  
	choice.age_groups_desc,  
	choice.title_desc,  
	choice.features_desc,  
	choice.details_and_care_desc,  
	choice.ingredients_desc,  
	image.image_index,  
	image.shot_name,  
	image.is_hero_image,  
	image.asset_id,  
	image.image_url
FROM {deg_t2_schema}.FASHION_MAP_CHOICE choice  
inner JOIN {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES image  
ON trim(choice.web_style_num) = trim(image.web_style_num)  
AND trim(choice.color_num) = trim(image.color_num)
AND trim(choice.channel_brand) = trim(image.channel_brand)
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS;




-- Compare new data to old table to find what changed that would impact the embeddings. Capture these and inset into the embeding update table
CREATE MULTISET VOLATILE TABLE vt_embedding_updated_products AS ( 
select
	distinct
	trim(nw.web_style_num) as web_style_num,
	nw.color_num,
	nw.channel_brand
from vt_new_choice nw
inner join vt_old_choice od
on CAST(nw.web_style_num AS VARCHAR(255)) = cast(trim(od.web_style_num) as varchar(255))
	and CAST(nw.color_num AS VARCHAR(255)) = CAST(od.color_num AS VARCHAR(255))
	and CAST(nw.image_index AS VARCHAR(255)) = CAST(od.image_index AS VARCHAR(255))
	and CAST(nw.is_hero_image AS VARCHAR(255)) = CAST(od.is_hero_image AS VARCHAR(255))
	and CAST(nw.channel_brand AS VARCHAR(255)) = CAST(od.channel_brand AS VARCHAR(255))
where 
	(
	CAST(nw.vendor_brand_name AS VARCHAR(255)) <> CAST(od.vendor_brand_name AS VARCHAR(255))
	or CAST(nw.type_level_1_desc AS VARCHAR(255)) <> CAST(od.type_level_1_desc AS VARCHAR(255))
	or CAST(nw.type_level_2_desc AS VARCHAR(255)) <> CAST(od.type_level_2_desc AS VARCHAR(255))
	or CAST(nw.genders_desc AS VARCHAR(255)) <> CAST(od.genders_desc AS VARCHAR(255))
	or CAST(nw.age_groups_desc AS VARCHAR(255)) <> CAST(od.age_groups_desc AS VARCHAR(255))
	or CAST(nw.title_desc AS VARCHAR(255)) <> CAST(od.title_desc AS VARCHAR(255))
	or CAST(nw.features_desc AS VARCHAR(255)) <> CAST(od.features_desc AS VARCHAR(255))
	or CAST(nw.details_and_care_desc AS VARCHAR(255)) <> CAST(od.details_and_care_desc AS VARCHAR(255))
	or CAST(nw.ingredients_desc AS VARCHAR(255)) <> CAST(od.ingredients_desc AS VARCHAR(255))
	or CAST(nw.asset_id AS VARCHAR(255)) <> CAST(od.asset_id AS VARCHAR(255))
	)
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS
;

-- Insert the updated products into the embedding update table
INSERT INTO {deg_t2_schema}.FASHION_MAP_CHOICE_EMBEDDING_UPDATES
select 
	trim(web_style_num) as web_style_num,
    color_num,    
    channel_brand,   
    CURRENT_DATE as dw_sys_load_date,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from vt_embedding_updated_products
;

-- Compare new data to old table to find what changed that would impact the selling status of the prodcut. Capture these and inset into the status update table
CREATE MULTISET VOLATILE TABLE vt_status_updated_products AS ( 
select
	distinct
	trim(nw.web_style_num) as web_style_num,
	nw.color_num,
	nw.channel_brand
from vt_new_choice nw
inner join vt_old_choice od
on CAST(nw.web_style_num AS VARCHAR(255)) = cast(trim(od.web_style_num) as varchar(255))
	and CAST(nw.color_num AS VARCHAR(255)) = CAST(od.color_num AS VARCHAR(255))
	and CAST(nw.channel_brand AS VARCHAR(255)) = CAST(od.channel_brand AS VARCHAR(255))
where 
	(
	nw.selling_retail_price_amt <> od.selling_retail_price_amt
	or nw.is_online_purchasable <> od.is_online_purchasable
	)
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS
;

-- Insert the updated products into the status update table
INSERT INTO {deg_t2_schema}.FASHION_MAP_CHOICE_STATUS_UPDATES
select 
	trim(web_style_num) as web_style_num,
    color_num,    
    channel_brand,   
    CURRENT_DATE as dw_sys_load_date,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from vt_status_updated_products
;

-- Find the new products 
---- Use the choice table as it is smaller
CREATE MULTISET VOLATILE TABLE vt_new_choice_products AS (  
SELECT  
	distinct 
    trim(web_style_num) as web_style_num,
    color_num,  
    channel_brand
from vt_new_choice
	where concat(channel_brand, '-', trim(web_style_num), color_num) not in (select concat(channel_brand, '-', trim(web_style_num), color_num) from {deg_t2_schema}.FASHION_MAP_CHOICE)
) WITH DATA 
PRIMARY INDEX (web_style_num) 
ON COMMIT PRESERVE ROWS
; 

-- INSERT THE NEW PRODUCTS INTO THE NEW TABLE 
INSERT INTO {deg_t2_schema}.FASHION_MAP_CHOICE_NEW
select 
	trim(web_style_num) as web_style_num, 
    color_num,  
    channel_brand,
    CURRENT_DATE as dw_sys_load_date,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from vt_new_choice_products
;

-- Now that we have the updated product and new products, we can overwrite the choice table with the newest data for the process to start over again the next day. 
DELETE FROM {deg_t2_schema}.FASHION_MAP_CHOICE
;

INSERT INTO {deg_t2_schema}.FASHION_MAP_CHOICE
select 
	distinct 
	trim(web_style_num) as web_style_num,
	rms_style_num,
    color_num,  
    channel_country,  
    channel_brand,   
    selling_channel,  
    selling_status_code,  
    selling_channel_eligibility_code,  
    is_online_purchasable,
    selling_retail_price_amt,
    regular_price_amt,   
    vendor_brand_name,
	UPPER(trim(type_level_1_desc)) as type_level_1_desc,
	UPPER(trim(type_level_2_desc)) as type_level_2_desc,
    genders_desc,  
    age_groups_desc,  
    title_desc,  
    features_desc,  
    details_and_care_desc,  
    ingredients_desc,
    CURRENT_DATE as dw_sys_load_date,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from vt_choice_dedupe
;

DELETE FROM {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES
;

INSERT INTO {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES
select 
	trim(web_style_num) as web_style_num,  
	trim(color_num) as color_num, 
    channel_country,  
	channel_brand,  
	image_index,  
    shot_name,
	is_hero_image,  
	asset_id,  
	image_url,
	CURRENT_DATE as dw_sys_load_date,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from vt_image_final
;

SET QUERY_BAND = NONE FOR SESSION
;
  