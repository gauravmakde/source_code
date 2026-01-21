SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=sku_dim_11521_ACE_ENG;
     Task_Name=sku_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.sku_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 10/28/2024

Note:
-- What is the the purpose of the table: SKU-level lookup table containing various item-specific & product hierarchy descriptions.
-- What is the the purpose of the sql: insert new active SKUs
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

--drop table sku_dim_prep;
CREATE VOLATILE TABLE sku_dim_prep AS 
(select distinct sk.rms_sku_num AS sku_id
                ,sk.epm_style_num
                ,sk.sku_desc
                ,sk.style_desc
                ,sk.brand_name
                ,sk.DIV_DESC as div_desc
                ,sk.DEPT_DESC as dept_desc
                ,sk.CLASS_DESC as class_desc
                ,sk.SBCLASS_DESC as sbclass_desc
 
                ,case when REGEXP_INSTR(sk.div_desc, 'WOMEN|WEAR|SPEC|MN/KID') > 0 then 'APPAREL'
                      when REGEXP_INSTR(sk.div_desc, 'SHOE') > 0 then 'SHOES'    
                      when REGEXP_INSTR(sk.div_desc, 'ACC') > 0 then 'ACCESSORIES'
                      when sk.div_desc is null then 'OTHER'
                      else OREPLACE(sk.div_desc,'INACT-|INACT -','')
                  end as merch_division
 
                ,REGEXP_REPLACE(
                     REGEXP_REPLACE(
                         REGEXP_REPLACE(
                             REGEXP_REPLACE(
                                 REGEXP_REPLACE(
                                     REGEXP_REPLACE(
                                         REGEXP_REPLACE(
                                             REGEXP_REPLACE(
                                                 REGEXP_REPLACE(
                                                     REGEXP_REPLACE(
                                                         REGEXP_REPLACE(
                                                             REGEXP_REPLACE(
                                                                 COALESCE(sk.dept_desc, 'OTHER')
                                                             , 'YNG ', 'YOUNG ')
                                                         , 'WMS |WMNS |WM ', 'WOMENS ')
                                                     , 'BTR |BET ', 'BETTER ')
                                                 , ' BTR| BET', ' BETTER ')
                                             , 'DSNR', 'DESIGNER')
                                         , 'SPTWR', 'SPORTSWEAR ')
                                     , 'HDBGS|HDBG', 'HANDBAGS ')
                                 , 'MID CRR', 'MID CAREER')
                             , 'ACT ', 'ACTIVE ')
                         , ' APP', ' APPAREL')
                     , '/LUG', '/LUGGAGE')
                     , 'BET ', 'BETTER ') as merch_department
 
                ,REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
                 REGEXP_REPLACE(
  
                       -- start:merch_class base
                       CASE WHEN sk.class_desc IN('DRESSES','SNEAKERS','DENIM PANTS','KNIT TOPS','BOOTS SHORT','SNEAKERS','GIFT SETS','SANDALS','FRAGRANCE','CROSSBODY','WM NONPZD SUNGL','CROSSBODY','TOTE','THROWS','HOME DECOR',
                                                  'SHEETS','HANDHELD','EARRINGS','HOME DECOR','TABLETOP','TOTE','CASUAL PANTS','DENIM','S/S WOVENS','SNEAKERS','KNIT TOPS') then sk.class_desc
                            WHEN sk.class_desc IS NULL THEN 'OTHER'
                            ELSE 'OTHER'
                        END
                       -- end:merch_class base
  
                ,'ACTIV ','ACTIVE')
                ,'ACCESSORY','ACCESSORIES')
                ,' ACC ',' ACCESSORIES ')
                ,'BOOT ','BOOTS ')
                ,'BOY ','BOYS ')
                ,'DRSS','DRESSES')
                ,'ELECTRNC','ELECTRONICS')
                ,' JWL ',' JEWELRY ')
                ,'GIRL |GRL ','GIRLS ')
                ,' GRL',' GIRLS')
                ,'ORGANIZATN','ORGANIZATION')
                ,'INF ','INFANTS ')
                ,'SANDAL |SNDL ','SANDALS ')
                ,'SNKR','SNEAKERS')
                ,' LIT | LITTL ',' LITTLE ')
                ,'TDLR','TODDLER')
                ,'YNG ','YOUNG ')
                ,'WMS |WMNS |WM ','WOMENS ')
                ,'BTR |BET ','BETTER ')
                ,' BTR| BET',' BETTER ')
                ,'DSNR','DESIGNER')
                ,'SPTWR','SPORTSWEAR ')
                ,'HDBGS|HDBG','HANDBAGS ')
                ,'MID CRR','MID CAREER')
                ,'ACT ','ACTIVE ')
                ,' APP',' APPAREL')
                ,'/LUG','/LUGGAGE')
                ,'BET ','BETTER ') as merch_class
  
                ,case when sk.brand_name in ('UGG','NIKE','SAINT LAURENT','GUCCI','ZELLA','FREE PEOPLE','TORY BURCH','ADIDAS','NORDSTROM','TORY BURCH','MAC DUGGAL','MADEWELL','NIKE','CHANEL','VINCE','TED BAKER LONDON','LA FEMME',
                                             'THE NORTH FACE','RAG AND BONE','AG','FRAME','DAVID YURMAN','PAIGE','NATORI','VINCE CAMUTO','SPANX','GUCCI','DYSON','CANADA GOOSE','LANCOME','ESTEE LAUDER','BONY LEVY','BAREFOOT DREAMS',
                                             'MARC JACOBS','NORDSTROM','DYSON','DOLCE AND GABBANA','ORREFORS','OLD TREND','CECE','BELLA BELLE','STONE ISLAND','DSQUARED2','THAMES AND KOSMOS','ECCO') then sk.brand_name
                      when sk.brand_name IS NULL then 'OTHER'
                      else 'OTHER'
                  end as merch_brand
  
                ,COALESCE(case when mpg.mpg_category    is null then 'undefined' else mpg.mpg_category    end, 'undefined') as merch_category_mpg
                ,COALESCE(case when dmr.merch_role_nord is null then 'undefined' else dmr.merch_role_nord end, 'undefined') as merch_role_nord
                ,COALESCE(case when dmr.merch_role_rack is null then 'undefined' else dmr.merch_role_rack end, 'undefined') as merch_role_rack
  
                ,sk.color_desc merch_color
                ,sk.size_1_desc merch_size
                ,sk.sku_type_desc merch_skutype
                ,pisrd.live_date
                ,sk.msrp_amt merch_price_msrp
                ,sty.style_group_num||'_'|| color_num merch_stylecolor_id
                ,sty.style_group_num
  
                ,case when REGEXP_INSTR(merch_category_mpg,'women') > 0 then 'womens'
                      when REGEXP_INSTR(merch_category_mpg,'men')   > 0 then 'mens'
                      when REGEXP_INSTR(merch_category_mpg,'kids')  > 0 then 'kids'
                      else cap.merch_persona
                  end merch_persona 
 
                ,case when REGEXP_INSTR(merch_category_mpg,'career')>0 then 'career'
                      when REGEXP_INSTR(merch_category_mpg,'intimates|sleep')>0 then 'sleep'
                      when REGEXP_INSTR(merch_category_mpg,'outerwear|active|swim')>0 then 'out and active'
                      when REGEXP_INSTR(merch_category_mpg,'fragrance|designer|fine_jewelry|handbags|beauty|mens_dress')>0 then 'dress to impress'
                      when REGEXP_INSTR(merch_category_mpg,'good|better|best')>0 then STRTOK(mpg_category,'_',4)
                      else 'other'
                  end merch_occasion
 
                ,cap.merch_type
                ,cap.inventory_capacity_category as smart_capacity_id
                ,concat(merch_division,'|',merch_department,'|',merch_class,'|',merch_brand,'|',coalesce(merch_role_nord,'undefined'),'|',coalesce(merch_role_rack,'undefined'),'|',coalesce(merch_category_mpg,'undefined')) as sku_segment_id
                ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from prd_nap_usr_vws.product_sku_dim as sk
  join (select distinct rms_sku_num from prd_nap_usr_vws.order_line_detail_fact oldf where oldf.order_date_pacific between {start_date} and {end_date} group by 1) as active_skus /* (current_date-3, current_date) */
    on sk.rms_sku_num = active_skus.rms_sku_num
  left join (select rms_sku_num
                   ,max(CAST(eff_begin_tmstp AS DATE)) as live_date
              from prd_nap_usr_vws.product_item_selling_rights_dim
              where channel_country = 'US'
                and is_sellable_ind = 'Y'
                and selling_status_code = 'UNBLOCKED'
                and selling_channel = 'ONLINE'
              group by 1) as pisrd
    on sk.rms_sku_num = pisrd.rms_sku_num
  left join (select epm_sku_num, epm_style_num, rms_style_num, div_desc from prd_nap_usr_vws.product_sku_dim where channel_country = 'US' qualify row_number() over (partition by epm_sku_num order by channel_country desc, dw_batch_date desc) = 1) as psd
    on psd.epm_sku_num = sk.epm_sku_num
  left join (select epm_style_num, class_num, dept_num, style_group_num from prd_nap_usr_vws.product_style_dim where channel_country = 'US' qualify row_number() over (partition by epm_style_num order by channel_country desc, dw_batch_date desc) = 1) as sty
    on sty.epm_style_num = psd.epm_style_num
  left join t2dl_das_cal.customer_merch_mpg_dept_categories as mpg
    on sk.dept_num = mpg.dept_num
  left join (select div_desc
                   ,dept_desc
                   ,class_desc
                   ,sbclass_desc
                   ,coalesce(merch_role_nord,'undefined') as merch_role_nord
                   ,coalesce(merch_role_rack,'undefined') as merch_role_rack
              from (select div_desc
                          ,dept_desc
                          ,class_desc
                          ,sbclass_desc
                          ,max(nord_role_desc) as merch_role_nord
                          ,max(rack_role_desc) as merch_role_rack
                     from t2dl_das_po_visibility.ccs_merch_themes 
                     group by 1,2,3,4) as dmr_inner
            ) as dmr
    on dmr.div_desc = sk.div_desc
   and dmr.dept_desc = sk.dept_desc
   and dmr.class_desc = sk.class_desc
   and dmr.sbclass_desc = sk.sbclass_desc
  left join (select dd.dept_num
                   ,subdivision_name
                   ,case when REGEXP_INSTR(subdivision_name,'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') >0 then 'womens'
                         when REGEXP_INSTR(subdivision_name,'MENS|MN|MEN') >0 then 'mens'
                         when REGEXP_INSTR(subdivision_name,'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') >0 then 'kids'
                         else 'other'
                     end as merch_persona
                   ,case when REGEXP_INSTR(subdivision_name,'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') >0 then 'apparel'
                         when REGEXP_INSTR(subdivision_name,'SHOE') >0 then 'shoes'
                         when REGEXP_INSTR(subdivision_name,'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') >0 then 'accessories'
                         when REGEXP_INSTR(subdivision_name,'HOME|FURNISH') >0 then 'home'
                         else 'other'
                     end as merch_type
                   ,case when merch_type = 'home' then 'other home'
                         when REGEXP_INSTR(subdivision_name,'KIDS APPAREL|KIDS DESIGNER') >0 or merch_persona = 'kids' then 'kids apparel and accessories'
                         when subdivision_name IN('ACCESSORIES','MAKEUP&SKINCARE') then 'womens accessories'
                         when REGEXP_INSTR(subdivision_name,'WOMENS DESIGNER|LINGERIE|WOMENS SPECIAL') >0 then 'womens apparel'  
                         when REGEXP_INSTR(subdivision_name,'MENS DESIGNER|MENS SPECIALIZED|AM DESIGNER') >0 then 'mens apparel' 
                         when merch_persona = 'womens' AND merch_type = 'other' then 'womens apparel'
                         when merch_persona = 'other' AND merch_type = 'apparel' then 'womens apparel'
                         when merch_persona = 'mens' AND merch_type = 'other' then 'mens apparel'   
                         else concat(merch_persona,' ',merch_type)
                     end as inventory_capacity_category
                   ,count(*) as departments
              from prd_nap_usr_vws.department_dim as dd
              group by 1,2,3,4,5) as cap
    on cap.dept_num = sk.dept_num
  where sk.channel_country = 'US'
)with data primary index (sku_id) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (sku_id) on sku_dim_prep;

INSERT INTO {usl_t2_schema}.sku_dim
SELECT *
 FROM sku_dim_prep
 WHERE sku_id not in (select distinct sku_id from {usl_t2_schema}.sku_dim);

COLLECT STATISTICS COLUMN (sku_id) on {usl_t2_schema}.sku_dim;

SET QUERY_BAND = NONE FOR SESSION;